package operators;


import elements.GraphOp;
import elements.Op;
import elements.ReplicaState;
import helpers.MyOutputReflectionContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Operator that Wraps around another operator and implements some common logic
 * This common logic is Edge-to-Edge broadcast, triple-all-reduce Watermarking strategy
 *
 * @implNote This operator is also acting as HeadOperator for the feedback streams
 * @see OneInputUDFWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends AbstractStreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, FeedbackConsumer<StreamRecord<GraphOp>>, Input<GraphOp>, BoundedOneInput, OperatorEventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.iteration.operator.AbstractWrapperOperator.class);
    public static OutputTag<GraphOp> ITERATE_OUTPUT_TAG = new OutputTag<GraphOp>("iterate", TypeInformation.of(GraphOp.class));
    public static OutputTag<GraphOp> BACKWARD_OUTPUT_TAG = new OutputTag<GraphOp>("backward", TypeInformation.of(GraphOp.class));

    // TAKEN FROM FLINK ML

    protected final StreamOperatorParameters<GraphOp> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final Output<StreamRecord<GraphOp>> output; // General Output for all other connected components
    protected final StreamOperatorFactory<GraphOp> operatorFactory;
    protected final T wrappedOperator;
    protected final IterationID iterationId;
    protected final MailboxExecutor mailboxExecutor;
    protected final Context context;
    protected final InternalOperatorMetricGroup metrics;


    // OPERATOR POSITION RELATED

    protected final short position;
    protected final short totalLayers;
    protected final short operatorIndex;


    // MY ADDITIONS for Watermarking and broadcasting

    protected Output[] internalOutputs; // All of operator-to-operator outputs
    protected BroadcastOutput[] internalBroadcastOutputs; // All of operator-to-operator autputs
    protected OutputTag[] internalOutputTags; // All of the existing output tags
    protected List<Short> thisParts; // Keys hashed to this operator, last one is the MASTER

    // WATERMARKING
    protected PriorityQueue<Long> waitingSyncs; // Sync messages that need to resolved for watermark to proceed
    protected int numOfSafeWatermarks; // Number of safe watermarks
    protected Watermark waitingWatermark;
    protected Watermark currentWatermarkInIteration;

    // FINALIZATION
    protected boolean readyToGracefullyFinish;
    protected int numberOfFinishMessages;

    // CONSTRUCTOR
    public BaseWrapperOperator(
            StreamOperatorParameters<GraphOp> parameters,
            StreamOperatorFactory<GraphOp> operatorFactory,
            IterationID iterationID,
            short position,
            short totalLayers) {
        this.position = position;
        this.totalLayers = totalLayers;
        this.parameters = Objects.requireNonNull(parameters);
        this.streamConfig = Objects.requireNonNull(parameters.getStreamConfig());
        this.containingTask = Objects.requireNonNull(parameters.getContainingTask());
        this.output = Objects.requireNonNull(parameters.getOutput());
        this.operatorFactory = Objects.requireNonNull(operatorFactory);
        this.iterationId = Objects.requireNonNull(iterationID);
        this.mailboxExecutor = parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(TaskMailbox.MIN_PRIORITY);
        this.wrappedOperator =
                (T)
                        StreamOperatorFactoryUtil.createOperator(
                                operatorFactory,
                                (StreamTask) parameters.getContainingTask(),
                                streamConfig,
                                new ProxyOutput(output),
                                parameters.getOperatorEventDispatcher())
                                .f0;
        this.metrics = createOperatorMetricGroup(containingTask.getEnvironment(), streamConfig);
        this.operatorIndex = (short) containingTask.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
        this.waitingSyncs = new PriorityQueue<>(1000);
        this.numOfSafeWatermarks = 0;
        this.waitingWatermark = null;
        this.currentWatermarkInIteration = null;
        this.readyToGracefullyFinish = false;
        this.numberOfFinishMessages = 0;
        assignThisKeys();
        try {
            // Creat individual outputs and the context
            this.createIndividualOutputs(output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
            this.context = new Context();
        } catch (Exception e) {
            throw new RuntimeException("OutputTags cannot be properly set up");
        }
    }


    // GENERAL WRAPPER STUFF

    @Override
    public void open() throws Exception {
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
        wrappedOperator.open();
    }

    @Override
    public void finish() throws Exception {
        wrappedOperator.finish();
    }

    @Override
    public void close() throws Exception {
        wrappedOperator.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        wrappedOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
        return wrappedOperator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
        RecordingStreamTaskStateInitializer recordingStreamTaskStateInitializer =
                new RecordingStreamTaskStateInitializer(streamTaskStateManager);
        wrappedOperator.initializeState(recordingStreamTaskStateInitializer);
        checkState(recordingStreamTaskStateInitializer.lastCreated != null);
//        wrappedOperator.setCurrentKey(thisParts.get(0).toString());
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        wrappedOperator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        wrappedOperator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return wrappedOperator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return wrappedOperator.getOperatorID();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public Object getCurrentKey() {
        return wrappedOperator.getCurrentKey();
    }

    @Override
    public void setCurrentKey(Object key) {
        wrappedOperator.setCurrentKey(key);
    }


    public T getWrappedOperator() {
        return wrappedOperator;
    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        context.element = record;
    }


    // WATERMARKING & PROCESSING LOGIC

    /**
     * Actually process the watermark, for each received W this will be called 4 times at W+1, W+2, W+3.
     * Representing the elements.iterations of the Watermark in the stream
     *
     * @param mark Watermark
     */
    public abstract void processActualWatermark(Watermark mark) throws Exception;
    public abstract void processActualElement(StreamRecord<GraphOp> element) throws Exception;




    /**
     * for SYNC requests check if there is a waiting Sync message and acknowledge Watermarks if any are waiting
     */
    @Override
    public final void processElement(StreamRecord<GraphOp> element) throws Exception {
        processActualElement(element);
        if (element.getValue().getOp() == Op.SYNC) {
            this.waitingSyncs.removeIf(item -> item == element.getTimestamp());
            acknowledgeIfWatermarkIsReady();
        }
    }

    /**
     * See if the watermark of this operator can be called safe. In other words if all SYNC messages were received
     */
    public void acknowledgeIfWatermarkIsReady() throws Exception {
        if (currentWatermarkInIteration == null && waitingWatermark != null && (waitingSyncs.peek() == null || waitingSyncs.peek() > waitingWatermark.getTimestamp())) {
            // Broadcast ack of watermark to all other operators including itself
            currentWatermarkInIteration = waitingWatermark;
            waitingWatermark = null;
            StreamRecord<GraphOp> record = new StreamRecord<>(new GraphOp(Op.WATERMARK, (short) 0, null, currentWatermarkInIteration.getTimestamp()), currentWatermarkInIteration.getTimestamp());
            setKeyContextElement(record);
            context.broadcastElement(ITERATE_OUTPUT_TAG, record.getValue());
        }
    }

    /**
     * Record the watermark and try to acknowledge it if possible
     */
    @Override
    public final void processWatermark(Watermark mark) throws Exception {
        waitingWatermark = mark; // Assuming that watermarks are always increasing -> Can safely do this
        acknowledgeIfWatermarkIsReady();
    }

    /**
     * Record acknowledges watermark or else just process it normally
     */
    @Override
    public final void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getOp() == Op.WATERMARK) {
            if (++numOfSafeWatermarks == containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks()) {
                numOfSafeWatermarks = 0;
                if (element.getValue().getPartId() == 0) {
                    // This means that Master -> Replica SYNC were finished all elements are in place, call actual watermark
                    // However, do one more all-reduce before emitting the watermark. We give slack of one-hop operations
                    System.out.format("First Watermark %s\n", context.getPosition());
                    processActualWatermark(currentWatermarkInIteration);
                    element.getValue().setPartId((short) 1);
                    context.broadcastElement(ITERATE_OUTPUT_TAG, element.getValue());
                } else {
                    // Now the watermark can be finally emitted
                    if (currentWatermarkInIteration.getTimestamp() == Long.MAX_VALUE) {
                        readyToGracefullyFinish = true;
                        // This watermark was just a notice about the end of input, do not emit it
                        // Next layers will receive finalization blocks anyways
                    }
                    context.emitWatermark(currentWatermarkInIteration);
                    currentWatermarkInIteration = null;
                    acknowledgeIfWatermarkIsReady();
                }

            }
        } else {
            setKeyContextElement(element);
            processElement(element);
        }
    }

    /**
     * Blocking the finalization procedure untill all iteration requests have been processed
     *
     * @implNote See {@link this.processFeedback} to see when graceFullFinish is executed
     */
    @Override
    public void endInput() throws Exception {
        System.out.format("Entered Endblock %s\n", context.getPosition());
        while (!readyToGracefullyFinish) {
            // Normal data processing stops here, just consume the iteration mailbox untill the Long.MAXVALUE watermark is received
            mailboxExecutor.tryYield();
            Thread.sleep(200);
        }
        System.out.format("Excaped Endblock %s\n", context.getPosition());
    }

    // CONFIGURATION

    // Broadcast Context + Feedback Registration + MetricGroup + ProxyOutput + This Parts
    private InternalOperatorMetricGroup createOperatorMetricGroup(
            Environment environment, StreamConfig streamConfig) {
        try {
            InternalOperatorMetricGroup operatorMetricGroup =
                    environment
                            .getMetricGroup()
                            .getOrAddOperator(
                                    streamConfig.getOperatorID(), streamConfig.getOperatorName());
            if (streamConfig.isChainEnd()) {
                operatorMetricGroup.getIOMetricGroup().reuseOutputMetricsForTask();
            }
            return operatorMetricGroup;
        } catch (Exception e) {
            LOG.warn("An error occurred while instantiating task metrics.", e);
            return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        }
    }

    /**
     * Calculate and assign all the parts mapped to this physical operator
     */
    private void assignThisKeys() {
        List<Short> thisKeys = new ArrayList<>();
        int index = operatorIndex;
        int maxParallelism = containingTask.getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks();
        int parallelism = containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
        for (short i = 0; i < maxParallelism; i++) {
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
            if (operatorIndex == index) {
                thisKeys.add(i);
            }
        }
        this.thisParts = thisKeys;
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = containingTask.getIndexInSubtaskGroup();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, 0);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<StreamRecord<GraphOp>> channel = broker.getChannel(key);
        OperatorUtils.registerFeedbackConsumer(channel, this, mailboxExecutor);
    }

    /**
     * Finding and creating the individual outputs
     */
    public void createIndividualOutputs(
            Output<StreamRecord<GraphOp>> output, Counter numRecordsOut) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method createInternalBroadcastOutput = BroadcastOutputFactory.class.getDeclaredMethod("createInternalBroadcastOutput", Output.class, OutputReflectionContext.class);
        createInternalBroadcastOutput.setAccessible(true);
        Output[] rawOutputs;
        BroadcastOutput[] rawBroadcastOutputs;
        OutputTag[] rawOutputTags;
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();

        if (myOutputReflectionContext.isBroadcastingOutput(output)) {
            rawOutputs =
                    myOutputReflectionContext.getBroadcastingInternalOutputs(output);
        } else {
            rawOutputs = new Output[]{output};
        }
        rawBroadcastOutputs = new BroadcastOutput[rawOutputs.length];
        rawOutputTags = new OutputTag[rawOutputs.length];
        for (int i = 0; i < rawOutputs.length; i++) {
            if (myOutputReflectionContext.isRecordWriterOutput(rawOutputs[i])) {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getRecordWriterOutputTag(rawOutputs[i]);
                rawOutputTags[i] = outputTag;
            } else {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getChainingOutputTag(rawOutputs[i]);
                rawOutputTags[i] = outputTag;
            }
            rawBroadcastOutputs[i] = (BroadcastOutput<GraphOp>) createInternalBroadcastOutput.invoke(null, rawOutputs[i], myOutputReflectionContext);
        }
        this.internalOutputs = rawOutputs;
        this.internalBroadcastOutputs = rawBroadcastOutputs;
        this.internalOutputTags = rawOutputTags;
        createInternalBroadcastOutput.setAccessible(false);
    }

    private static class RecordingStreamTaskStateInitializer implements StreamTaskStateInitializer {

        private final StreamTaskStateInitializer wrapped;

        StreamOperatorStateContext lastCreated;

        public RecordingStreamTaskStateInitializer(StreamTaskStateInitializer wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public StreamOperatorStateContext streamOperatorStateContext(
                @Nonnull OperatorID operatorID,
                @Nonnull String s,
                @Nonnull ProcessingTimeService processingTimeService,
                @Nonnull KeyContext keyContext,
                @Nullable TypeSerializer<?> typeSerializer,
                @Nonnull CloseableRegistry closeableRegistry,
                @Nonnull MetricGroup metricGroup,
                double v,
                boolean b)
                throws Exception {
            lastCreated =
                    wrapped.streamOperatorStateContext(
                            operatorID,
                            s,
                            processingTimeService,
                            keyContext,
                            typeSerializer,
                            closeableRegistry,
                            metricGroup,
                            v,
                            b);
            return lastCreated;
        }
    }

    /**
     * ProxyOutput are outputs of underlying operators in order to disable them from doing any watermarking and higher-level logic
     * Those stuff should be handled by the oeprator implementing this class
     * Also the timestamps are assigned by the GraphOp Timestamps
     */
    public class ProxyOutput implements Output<StreamRecord<GraphOp>> {
        Output<StreamRecord<GraphOp>> output;

        ProxyOutput(Output<StreamRecord<GraphOp>> output) {
            this.output = output;
        }

        @Override
        public void emitWatermark(Watermark mark) {
            // Pass because watermarks should be emitted by the wrapper operators
//            output.emitWatermark(mark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            output.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            if (outputTag == ITERATE_OUTPUT_TAG && record.getValue() instanceof GraphOp) {
                GraphOp el = (GraphOp) record.getValue();
                if (el.getOp() == Op.SYNC && el.element.state() == ReplicaState.REPLICA) {
                    // Replica wants to sync with master
                    waitingSyncs.add(record.getTimestamp());
                }
            }
            output.collect(outputTag, record);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void collect(StreamRecord<GraphOp> record) {
            output.collect(record);
        }

        @Override
        public void close() {
            output.close();
        }
    }

    /**
     * Context is used to have more fine grained control over where to send watermarks
     */
    public class Context {
        StreamRecord<GraphOp> element = null;

        /**
         * Send watermark exactly to one output channel
         *
         * @implNote if @param outputTag is null, will send it to forward channel
         */
        protected void emitWatermark(@Nullable OutputTag<?> outputTag, Watermark e) {
            for (int i = 0; i < internalOutputTags.length; i++) {
                if (Objects.equals(outputTag, internalOutputTags[i])) {
                    internalOutputs[i].emitWatermark(e);
                }
            }
        }

        /**
         * Emit watermark to all channels
         */
        protected void emitWatermark(Watermark e) {
            for (Output internalOutput : internalOutputs) {
                internalOutput.emitWatermark(e);
            }
        }

        /**
         * Broadcast element to one input channel
         *
         * @param outputTag outputTag or null for broadcasting to forward operator
         * @param el        Element
         */
        public <OUT> void broadcastElement(@Nullable OutputTag<OUT> outputTag, OUT el) {
            StreamRecord<OUT> record = new StreamRecord<>(el, element.getTimestamp());
            for (int i = 0; i < internalOutputTags.length; i++) {
                if (Objects.equals(outputTag, internalOutputTags[i])) {
                    try {
                        internalBroadcastOutputs[i].broadcastEmit(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Horizontal Position of this operator
         *
         * @return horizontal position
         */
        public short getPosition() {
            return position;
        }

        /**
         * Number of operators chained together horizontally
         *
         * @return layers number
         */
        public short getNumLayers() {
            return totalLayers;
        }


    }

}
