package operators;


import elements.GraphOp;
import elements.Op;
import elements.iterations.MessageCommunication;
import helpers.MyOutputReflectionContext;
import operators.events.IterableOperatorEvent;
import operators.events.WatermarkEvent;
import operators.events.WatermarkStatusEvent;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
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
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Operator that Wraps around another operator and implements some common logic
 * This common logic is Edge-to-Edge broadcast, triple-all-reduce Watermarking strategy
 *
 * @implNote This operator is also acting as HeadOperator for the feedback streams
 * @see UdfWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends AbstractStreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, Input<GraphOp>, BoundedOneInput, OperatorEventHandler, StreamOperatorStateHandler.CheckpointedStreamOperator {

    /**
     * STATIC PROPS
     */

    protected static final Logger LOG = LoggerFactory.getLogger(BaseWrapperOperator.class);

    public static OutputTag<GraphOp> ITERATE_OUTPUT_TAG = new OutputTag<GraphOp>("iterate", TypeInformation.of(GraphOp.class));

    public static OutputTag<GraphOp> BACKWARD_OUTPUT_TAG = new OutputTag<GraphOp>("backward", TypeInformation.of(GraphOp.class));

    public static OutputTag<GraphOp> FULL_ITERATE_OUTPUT_TAG = new OutputTag<GraphOp>("full-iterate", TypeInformation.of(GraphOp.class));


    /**
     * OPERATOR PROPS
     */

    protected final StreamOperatorParameters<GraphOp> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final Output<StreamRecord<GraphOp>> output; // General Output for all other connected components

    protected final StreamOperatorFactory<GraphOp> operatorFactory;

    protected final T wrappedOperator;

    protected final Context context;

    protected final InternalOperatorMetricGroup metrics;

    protected final OperatorEventGateway operatorEventGateway; // Event gateway

    protected final short position; // Horizontal position

    protected final short operatorIndex; // Vertical position

    protected final short totalLayers; // Total horizontal layers

    protected final IterationID iterationID;

    protected short ITERATION_COUNT; // How many times elements are expected to iterate in this stream

    protected transient StreamOperatorStateHandler stateHandler; // State handler similar to the AbstractStreamOperator



    /**
     *  Watermarking, Broadcasting, Partitioning CheckPoiting PROPS
     */

    protected Output[] internalOutputs; // All of operator-to-operator outputs

    protected BroadcastOutput[] internalBroadcastOutputs; // All of operator-to-operator broadcast outputs

    protected OutputTag[] internalOutputTags; // All the existing output tags. In other words connected to this operator

    protected int[] numOutChannels; // number of output channels per each operator

    protected List<Short> thisParts; // Part Keys hashed to this operator, first one is regarded MASTER key. Used in broadcast outputs

    protected HashMap<IterableOperatorEvent, Short> events;

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    // MAIN CONSTRUCTOR
    public BaseWrapperOperator(
            StreamOperatorParameters<GraphOp> parameters,
            StreamOperatorFactory<GraphOp> operatorFactory,
            IterationID iterationID,
            short position,
            short totalLayers,
            short iterationCount) {
        this.position = position;
        this.iterationID = iterationID;
        this.totalLayers = totalLayers;
        this.ITERATION_COUNT = iterationCount;
        this.parameters = Objects.requireNonNull(parameters);
        this.streamConfig = Objects.requireNonNull(parameters.getStreamConfig());
        this.containingTask = Objects.requireNonNull(parameters.getContainingTask());
        this.output = Objects.requireNonNull(parameters.getOutput());
        this.operatorFactory = Objects.requireNonNull(operatorFactory);
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
        this.events = new HashMap<>();
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        this.addFeedbackConsumer();
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(
                        getOperatorID(), this);
        assignKeys(); // Should be before the context is initialized
        this.context = new Context();
        try {
            createIndividualOutputs(output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
        } catch (Exception e) {
            new RuntimeException("OutputTags cannot be properly set up").printStackTrace();
            throw new RuntimeException();
        }
    }


    /**
     * GENERAL WRAPPER FUNCTIONS
     */

    /**
     *
     */
    @Override
    public void open() throws Exception {
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

    /**
     * Subclasses should implement the wrapper logic
     */
    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        context.element = record;
    }

    @Override
    public final void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker(latencyMarker);
    }


    /**
     * SNAPSHOTTING AND CHECKPOINTING
     */

    /**
     * This callback is called first before having a checkpoint
     * Stops the consumer from reading the feedback streams
     */
    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        wrappedOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    /**
     * Initializing the state of this wrapper only not the underlying operator
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // Pass
    }

    /**
     * Snapshotting the state of this wrapper only not the underlying operator
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Pass
    }

    @Override
    public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
        return wrappedOperator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public final void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
        RecordingStreamTaskStateInitializer recordingStreamTaskStateInitializer =
                new RecordingStreamTaskStateInitializer(streamTaskStateManager);
        wrappedOperator.initializeState(recordingStreamTaskStateInitializer);
        checkState(recordingStreamTaskStateInitializer.lastCreated != null);
        stateHandler = new StreamOperatorStateHandler(recordingStreamTaskStateInitializer.lastCreated, containingTask.getExecutionConfig(), containingTask.getCancelables());
        stateHandler.initializeOperatorState(this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointAborted(checkpointId);
    }


    /**
     * ABSTRACT FUNCTIONS
     */

    /**
     * Actually process the element
     */
    public abstract void processActualElement(StreamRecord<GraphOp> element) throws Exception;

    /**
     * WATERMARKING PROCESSING ENDING THE STREAM LOGIC
     */

    /**
     * Process the watermark status and iterate
     */
    @Override
    public final void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        WatermarkStatusEvent e = new WatermarkStatusEvent(watermarkStatus.status, ITERATION_COUNT);
        GraphOp op = new GraphOp(Op.OPERATOR_EVENT, thisParts.get(0), null, e, MessageCommunication.BROADCAST, null);
        StreamRecord<GraphOp> tmp = new StreamRecord<>(op, context.element.getTimestamp());
        events.put(e, (short) (containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks() - 1));
        processElement(tmp);
    }

    /**
     * Record the watermark and try to acknowledge it if possible
     */
    @Override
    public final void processWatermark(Watermark mark) throws Exception {
        WatermarkEvent e = new WatermarkEvent(mark.getTimestamp(), ITERATION_COUNT);
        GraphOp op = new GraphOp(Op.OPERATOR_EVENT, thisParts.get(0), null, e, MessageCommunication.BROADCAST, null);
        StreamRecord<GraphOp> tmp = new StreamRecord<>(op, mark.getTimestamp());
        events.put(e, (short) (containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks() - 1));
        processElement(tmp);
    }

    /**
     * Process events from coordinator. Some might be iterable hence will need iteration
     *
     * @param evt Coordinator event
     */
    @Override
    public final void handleOperatorEvent(OperatorEvent evt) {
        try {
            if (evt instanceof IterableOperatorEvent)
                ((IterableOperatorEvent) evt).setCurrentIteration(ITERATION_COUNT);
            GraphOp op = new GraphOp(Op.OPERATOR_EVENT, thisParts.get(0), null, evt, MessageCommunication.BROADCAST, null);
            StreamRecord<GraphOp> tmp = new StreamRecord<>(op, context.element.getTimestamp());
            processElement(tmp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Record acknowledges watermark/watermarkStatus or else just process it normally
     */
    @Override
    public final void processElement(StreamRecord<GraphOp> element) throws Exception {
        setKeyContextElement(element);
        if (element.getValue().getOp() == Op.OPERATOR_EVENT && element.getValue().getOperatorEvent() instanceof IterableOperatorEvent) {
            IterableOperatorEvent ev = (IterableOperatorEvent) element.getValue().getOperatorEvent();
            if (Objects.isNull(ev.getCurrentIteration())) {
                // This event is received from previous operator, not iteration messages nor generated using above functions
            } else {
                if (events.merge(ev, (short) 1, (a, b) -> (short) (a + b)) == containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks()) {
                    // Process This Iteration
                    processActualElement(element);
                    events.remove(ev);
                    if (ev.currentIteration == 0) {
                        if(context.getPosition()>0){
                            System.out.println("");
                        }
                        // pass, do not do anymore iterations
                        if (ev instanceof WatermarkEvent) {
                            Watermark mark = ((WatermarkEvent) ev).getWatermark();
                            wrappedOperator.processWatermark(mark);
                        } else if (ev instanceof WatermarkStatusEvent) {
                            WatermarkStatus status = ((WatermarkStatusEvent) ev).getWatermarkStatus();
                            wrappedOperator.processWatermarkStatus(status);
                        }
                    } else {
                        ev.currentIteration--;
                        output.collect(ITERATE_OUTPUT_TAG, element);
                    }
                }
            }
        } else {
            processActualElement(element);
        }
    }

    /**
     * Blocking the finalization procedure untill all iteration requests have been processed
     *
     * @implNote See {@link this.processFeedback} to see when graceFullFinish is executed
     */
    @Override
    public void endInput() throws Exception {

    }

    /**
     * SETUP OF: BROADCAST Context + Feedback Registration + MetricGroup + ProxyOutput + This Parts
     */

    /**
     * Create metric group for this operator
     */
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
    private void assignKeys() {
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
        if (thisKeys.isEmpty())
            throw new IllegalStateException("Make sure Partitioner keys are large enough to fill physical partitioning");
        this.thisParts = thisKeys;
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
        int[] rawNumOutChannels;
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();

        if (myOutputReflectionContext.isBroadcastingOutput(output)) {
            rawOutputs =
                    myOutputReflectionContext.getBroadcastingInternalOutputs(output);
        } else {
            rawOutputs = new Output[]{output};
        }
        rawBroadcastOutputs = new BroadcastOutput[rawOutputs.length];
        rawOutputTags = new OutputTag[rawOutputs.length];
        rawNumOutChannels = new int[rawOutputs.length];
        for (int i = 0; i < rawOutputs.length; i++) {
            if (myOutputReflectionContext.isRecordWriterOutput(rawOutputs[i])) {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getRecordWriterOutputTag(rawOutputs[i]);
                rawOutputTags[i] = outputTag;
                rawNumOutChannels[i] = myOutputReflectionContext.getNumChannels(rawOutputs[i]);
            } else {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getChainingOutputTag(rawOutputs[i]);
                rawNumOutChannels[i] = containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
                rawOutputTags[i] = outputTag;
            }
            rawBroadcastOutputs[i] = (BroadcastOutput<GraphOp>) createInternalBroadcastOutput.invoke(null, rawOutputs[i], myOutputReflectionContext);
        }
        this.internalOutputs = rawOutputs;
        this.internalBroadcastOutputs = rawBroadcastOutputs;
        this.internalOutputTags = rawOutputTags;
        this.numOutChannels = rawNumOutChannels;
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
     * ProxyOutput are outputs of underlying operators in order to disable them from doing any watermarking and lower-level logic
     * Those stuff should be handled by the operator implementing this class
     */
    public class ProxyOutput implements Output<StreamRecord<GraphOp>> {
        Output<StreamRecord<GraphOp>> output;

        ProxyOutput(Output<StreamRecord<GraphOp>> output) {
            this.output = output;
        }

        @Override
        public void emitWatermark(Watermark mark) {
            // Pass because watermarks should be emitted by the wrapper operators
            output.emitWatermark(mark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            // Pass because watermark status should be emitted by the wrapper operator
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
        StreamRecord<GraphOp> element = new StreamRecord<>(new GraphOp(Op.NONE, thisParts.get(0), null, null));

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
            output.emitWatermark(e);
        }

        protected void emitWatermarkStatus(WatermarkStatus s) {
            output.emitWatermarkStatus(s);
        }

        /**
         * Send event to operator
         *
         * @param e Event
         */
        public void sendOperatorEvent(OperatorEvent e) {
            operatorEventGateway.sendEventToCoordinator(e);
        }

        /**
         * Get the number of output channels for the given OutputTag or null if next layer
         */
        public int getNumberOfOutChannels(@Nullable OutputTag<?> outputTag) {
            for (int i = 0; i < internalOutputTags.length; i++) {
                if (Objects.equals(outputTag, internalOutputTags[i])) {
                    return numOutChannels[i];
                }
            }
            new Exception("OutChannels not found").printStackTrace();
            return -1;
        }

        /**
         * Get the current part of this operator
         */
        public Short currentPart() {
            return element.getValue().getPartId();
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

    private void addFeedbackConsumer() {
        int indexOfThisSubtask = this.containingTask.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(this.iterationID, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, this.containingTask.getEnvironment().getTaskInfo().getAttemptNumber());
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.feedbackChannel = broker.getChannel(key);
    }

}
