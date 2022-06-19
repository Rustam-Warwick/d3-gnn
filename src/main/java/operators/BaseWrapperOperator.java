package operators;


import elements.GraphOp;
import elements.Op;
import elements.iterations.MessageCommunication;
import helpers.MyOutputReflectionContext;
import operators.events.IterableOperatorEvent;
import operators.events.WatermarkEvent;
import operators.events.WatermarkStatusEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.broadcast.OutputReflectionContext;
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
import org.apache.flink.runtime.state.*;
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
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Operator that Wraps around another operator and implements some common logic
 * This common logic is Edge-to-Edge broadcast, triple-all-reduce Watermarking strategy
 *
 * @implNote This operator is also acting as HeadOperator for the feedback streams
 * @see UdfWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends AbstractStreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, Input<GraphOp>, OperatorEventHandler, StreamOperatorStateHandler.CheckpointedStreamOperator {

    /**
     * STATIC PROPS
     */

    public static final Logger LOG = LoggerFactory.getLogger(BaseWrapperOperator.class);

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

    protected final IterationID iterationID; // Id for the Iteration

    protected short ITERATION_COUNT; // How many times elements are expected to iterate in this stream

    protected transient StreamOperatorStateHandler stateHandler; // State handler similar to the AbstractStreamOperator


    /**
     * Watermarking, Broadcasting, Partitioning CheckPoiting PROPS
     */

    protected transient Output[] internalOutputs; // All of operator-to-operator outputs

    protected transient BroadcastOutput[] internalBroadcastOutputs; // All of operator-to-operator broadcast outputs

    protected transient OutputTag[] internalOutputTags; // All the existing output tags. In other words connected to this operator

    protected transient int[] numOutChannels; // number of output channels per each operator

    protected transient List<Short> thisParts; // Part Keys hashed to this operator, first one is regarded MASTER key. Used in broadcast outputs

    protected transient List<Short> otherMasterParts; // Parts that are mapped to other operators

    protected transient Map<IterableOperatorEvent, Short> events; // Table of IterableOperatorEvents received by this operator

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
                                output,
                                parameters.getOperatorEventDispatcher())
                                .f0;
        this.metrics = createOperatorMetricGroup(containingTask.getEnvironment(), streamConfig);
        this.operatorIndex = (short) containingTask.getEnvironment().getTaskInfo().getIndexOfThisSubtask();
        this.events = new HashMap<>();
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(
                        getOperatorID(), this);
        try {
            createIndividualOutputs(output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
            calculateParts(); // Should be before the context is initialized
            context = new Context();
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
     * Actually process the element, should be implemented by the sub operators
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
                if (events.merge(ev, (short) 1, (a, b) -> (short) (a + b)) >= containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks()) {
                    // Process This Iteration
                    processActualElement(element);
                    events.remove(ev);
                    if (ev.currentIteration == 0) {
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
     * Calculate and assign all the parts mapped to this physical operator, and other master parts
     */
    private void calculateParts() {
        thisParts = new ArrayList<>();
        otherMasterParts = new ArrayList<>();
        int index = operatorIndex;
        int maxParallelism = containingTask.getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks();
        int parallelism = containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
        boolean[] seen = new boolean[parallelism];
        for (short i = 0; i < maxParallelism; i++) {
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(PartNumber.of(i), maxParallelism, parallelism);
            if (operatorIndex == index) {
                thisParts.add(i);
            } else if (!seen[operatorIndex]) {
                otherMasterParts.add(i);
            }
            seen[operatorIndex] = true;
        }
        if (thisParts.isEmpty())
            throw new IllegalStateException("Make sure Partitioner keys are large enough to fill physical partitioning");
    }

    /**
     * Finding and creating the individual outputs, broadcast outputs, parallelisms of our outputs
     */
    public void createIndividualOutputs(
            Output<StreamRecord<GraphOp>> output, Counter numRecordsOut) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method createInternalBroadcastOutput = BroadcastOutputFactory.class.getDeclaredMethod("createInternalBroadcastOutput", Output.class, OutputReflectionContext.class);
        createInternalBroadcastOutput.setAccessible(true);
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();

        if (myOutputReflectionContext.isBroadcastingOutput(output)) {
            internalOutputs =
                    myOutputReflectionContext.getBroadcastingInternalOutputs(output);
        } else {
            internalOutputs = new Output[]{output};
        }
        internalBroadcastOutputs = new BroadcastOutput[internalOutputs.length];
        internalOutputTags = new OutputTag[internalOutputs.length];
        numOutChannels = new int[internalOutputs.length];
        for (int i = 0; i < internalOutputs.length; i++) {
            if (myOutputReflectionContext.isRecordWriterOutput(internalOutputs[i])) {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getRecordWriterOutputTag(internalOutputs[i]);
                internalOutputTags[i] = outputTag;
                numOutChannels[i] = myOutputReflectionContext.getNumChannels(internalOutputs[i]);
            } else {
                OutputTag<GraphOp> outputTag = (OutputTag<GraphOp>) myOutputReflectionContext.getChainingOutputTag(internalOutputs[i]);
                numOutChannels[i] = containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
                internalOutputTags[i] = outputTag;
            }
            internalBroadcastOutputs[i] = (BroadcastOutput<GraphOp>) createInternalBroadcastOutput.invoke(null, internalOutputs[i], myOutputReflectionContext);
        }
        createInternalBroadcastOutput.setAccessible(false);
    }

    /**
     * Stream Task initializer for the wrapped operator
     * Needed if we need to initialize some state in this operator as well
     */
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
     * Context is used to have more fine grained control over where to send watermarks
     */
    public class Context {
        StreamRecord<GraphOp> element = new StreamRecord<>(new GraphOp(Op.NONE, thisParts.get(0), null, null));

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

        /**
         * Get parts hashed to this operator
         */
        public List<Short> getThisOperatorParts() {
            return thisParts;
        }

        /**
         * Master parts are defined as the first parts that are mapped to other operators
         *
         * @return
         */
        public List<Short> getOtherOperatorMasterParts() {
            return otherMasterParts;
        }


        public void applyToAllKeys(Runnable o) throws Exception {
            Short tmp = element.getValue().getPartId();
            for (Short thisPart : thisParts) {
                element.getValue().setPartId(thisPart);
                setKeyContextElement(element);
                o.run();
            }
            element.getValue().setPartId(tmp);
            setKeyContextElement(element);
        }
    }

}
