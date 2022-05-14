package operators;


import elements.GraphOp;
import elements.Op;
import helpers.MyOutputReflectionContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Operator that Wraps around another operator and implements some common logic
 * This common logic is Edge-to-Edge broadcast, triple-all-reduce Watermarking strategy
 *
 * @implNote This operator is also acting as HeadOperator for the feedback streams
 * @see OneInputUDFWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends StreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, FeedbackConsumer<StreamRecord<GraphOp>>, Input<GraphOp> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.iteration.operator.AbstractWrapperOperator.class);
    public static OutputTag<GraphOp> ITERATE_OUTPUT_TAG = new OutputTag<GraphOp>("iterate", TypeInformation.of(GraphOp.class));
    public static OutputTag<GraphOp> BACKWARD_OUTPUT_TAG = new OutputTag<GraphOp>("backward", TypeInformation.of(GraphOp.class));

    // ---- Configuration details
    protected final StreamOperatorParameters<GraphOp> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final Output<StreamRecord<GraphOp>> output;
    protected final StreamOperatorFactory<GraphOp> operatorFactory;
    protected final T wrappedOperator;
    protected final IterationID iterationId;
    protected final MailboxExecutor mailboxExecutor;
    protected final Context context;
    protected final InternalOperatorMetricGroup metrics;
    // --------- Position and watermarking related stuff
    protected final short position;
    protected final short totalLayers;
    protected final short operatorIndex;
    // -------- Additional outputs by me
    protected Output[] internalOutputs;
    protected BroadcastOutput[] internalBroadcastOutputs;
    protected OutputTag[] internalOutputTags;
    protected List<Short> thisParts;
    private boolean watermarkInIteration = false;
    private Watermark waitingWatermark = null;


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
                                new ProxyOutput<>(output),
                                parameters.getOperatorEventDispatcher())
                                .f0;

        this.metrics = createOperatorMetricGroup(containingTask.getEnvironment(), streamConfig);
        this.operatorIndex = (short) containingTask.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
        assignThisKeys();
        try {
            // Creat individual outputs and the context
            this.createIndividualOutputs(output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
            this.context = new Context();
        } catch (Exception e) {
            throw new RuntimeException("OutputTags cannot be properly set up");
        }
    }

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
        wrappedOperator.initializeState(streamTaskStateManager);
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

    // WATERMARKING

    public abstract void processActualWatermark(Watermark mark) throws Exception;

    @Override
    public final void processWatermark(Watermark mark) throws Exception {
        if (watermarkInIteration) {
            if (waitingWatermark == null) waitingWatermark = mark;
            else {
                waitingWatermark = new Watermark(Math.max(waitingWatermark.getTimestamp(), mark.getTimestamp()));
            }
        } else {
            Watermark iterationWatermark = new Watermark(mark.getTimestamp() - (mark.getTimestamp() % 4)); // Normalize to have remainder 0
            watermarkInIteration = true;
            context.emitWatermark(BaseWrapperOperator.ITERATE_OUTPUT_TAG, iterationWatermark);
        }
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getOp() == Op.WATERMARK) {
            short iterationNumber = (short) (element.getTimestamp() % 4);
            Watermark newWatermark = new Watermark(element.getTimestamp() + 1);
            if (iterationNumber < 2) {
                // Still need to traverse the stream before updating the timer
                context.emitWatermark(BaseWrapperOperator.ITERATE_OUTPUT_TAG, newWatermark);
            } else {
                // Watermark is ready to be consumed, before consuming do onWatermark on all the keyed elements
                processActualWatermark(newWatermark);
                context.emitWatermark(newWatermark);
                watermarkInIteration = false;
                if (waitingWatermark != null) {
                    processWatermark(waitingWatermark);
                    waitingWatermark = null;
                }
            }
        } else {
            setKeyContextElement(element);
            processElement(element);
        }
    }

    // CONFIGURATION Context + Feedback + MetricGroup + ProxyOutput + This Parts
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

    /**
     * ProxyOutput are outputs of underlying operators in order to disable them from doing any watermarking and higher-level logic
     * Those stuff should be handled by the oeprator implementing this class
     * Also the timestamps are assigned by the GraphOp Timestamps
     *
     * @param <T> TypeOf the proxu output
     */
    public static class ProxyOutput<T> implements Output<T> {
        Output<T> output;

        ProxyOutput(Output<T> output) {
            this.output = output;
        }

        @Override
        public void emitWatermark(Watermark mark) {
            // Pass because watermarks should be emitted by the wrapper operators
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            output.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            output.collect(outputTag, record);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void collect(T record) {
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
        public <OUT> void broadcastElement(@Nullable OutputTag<OUT> outputTag, StreamRecord<OUT> el) {
            for (int i = 0; i < internalOutputTags.length; i++) {
                if (Objects.equals(outputTag, internalOutputTags[i])) {
                    try {
                        internalBroadcastOutputs[i].broadcastEmit(el);
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
