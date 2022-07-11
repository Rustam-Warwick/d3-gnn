package operators;


import elements.GraphOp;
import elements.Op;
import elements.iterations.MessageCommunication;
import helpers.MyOutputReflectionContext;
import operators.events.BaseOperatorEvent;
import operators.events.FlowingOperatorEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.RecordWriterBroadcastOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Operator that Wraps around another operator and implements some common logic
 * @implNote Assumes that the input is GraphOp
 * @implNote Assumes that if the input is keyed it should be KeyedBy PartNumber
 * @see GNNLayerWrapperOperator manages wrapping around single operators
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

    /**
     * My additions for GNN
     */

    protected final short position; // Horizontal position

    protected final short operatorIndex; // Vertical position

    protected final short parallelism; // Parallelism of this operator

    protected final short totalLayers; // Total horizontal layers

    protected final IterationID iterationID; // Id for the Iteration

    protected transient StreamOperatorStateHandler stateHandler; // State handler similar to the AbstractStreamOperator

    /**
     * Watermarking, Broadcasting, Partitioning CheckPoiting PROPS
     */

    protected transient OutputTag<?>[] internalOutputTags; // All the existing output tags. In other words connected to this operator

    protected transient BroadcastOutput[] broadcastOutputs; // Broadcasting to the next operator only

    protected transient int[] numOutChannels; // number of output channels per each operator

    protected transient List<Short> thisParts; // Part Keys hashed to this operator, first one is regarded MASTER key. Used in broadcast outputs

    protected transient List<Short> otherMasterParts; // Master Parts that are mapped to other operators

    protected transient Map<FlowingOperatorEvent, Short> events; // Table of FlowingOperatorEvents received by this operator

    // MAIN CONSTRUCTOR
    public BaseWrapperOperator(
            StreamOperatorParameters<GraphOp> parameters,
            StreamOperatorFactory<GraphOp> operatorFactory,
            IterationID iterationID,
            short position,
            short totalLayers) {
        this.position = position;
        this.iterationID = iterationID;
        this.totalLayers = totalLayers;
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
        this.parallelism = (short) containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
        this.events = new HashMap<>();
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(getOperatorID(), this);
        createIndividualOutputs(output);
        calculateParts(); // Should be before the context is initialized
        context = new Context();
    }


    /**
     * GENERAL WRAPPER DELEGATION FUNCTIONS
     */

    /**
     *
     */
    @Override
    public void open() throws Exception {
        setKeyContextElement(context.element);
        wrappedOperator.open();
        for (int j = 0; j < 5; j++) {
            System.gc();
            System.runFinalization();
        }
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
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        if (record.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
            if (record.getValue().getPartId() == null) {
                // Broadcast messages should be sent with null id, this will assign the first part id to it
                record.getValue().setPartId(thisParts.get(0));
            }
        }
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
        wrappedOperator.processWatermarkStatus(watermarkStatus);
    }

    /**
     * Record the watermark and try to acknowledge it if possible
     */
    @Override
    public final void processWatermark(Watermark mark) throws Exception {
        wrappedOperator.processWatermark(mark);
    }

    /**
     * Operator Events in a Wrapper Operator are treated like broadcast events
     * This means that
     *
     * @param evt Coordinator event
     */
    @Override
    public final void handleOperatorEvent(OperatorEvent evt) {
        assert evt instanceof BaseOperatorEvent; // Only send BaseOperatorEvents
        try {
            GraphOp op = new GraphOp(Op.OPERATOR_EVENT, null, null, (BaseOperatorEvent) evt, MessageCommunication.BROADCAST, null);
            context.element.replace(op);
            setKeyContextElement(context.element);
            processElement(context.element);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * If processed element in FlowingOperatorEvent which actually came from the pipeline, need to aggregate its count and trigger only if all broadcasts have been received
     */
    @Override
    public final void processElement(StreamRecord<GraphOp> element) throws Exception {
        context.element = element;
        if (element.getValue().getOp() == Op.OPERATOR_EVENT && element.getValue().getOperatorEvent() instanceof FlowingOperatorEvent && Objects.nonNull(((FlowingOperatorEvent) element.getValue().getOperatorEvent()).getBroadcastCount())) {
            FlowingOperatorEvent ev = (FlowingOperatorEvent) element.getValue().getOperatorEvent();
            if (events.merge(ev, (short) 1, (a, b) -> (short) (a + b)) >= ev.getBroadcastCount()) {
                // Process This Iteration
                processActualElement(element);
                events.remove(ev);
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
        PartNumber reuse = PartNumber.of((short) 0);
        for (short i = 0; i < maxParallelism; i++) {
            reuse.setPartId(i);
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(reuse, maxParallelism, parallelism);
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
            Output<StreamRecord<GraphOp>> output){
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();
        Output<StreamRecord<Object>>[] internalOutputs;
        if (myOutputReflectionContext.isBroadcastingOutput(output)) {
            internalOutputs =
                    myOutputReflectionContext.getBroadcastingInternalOutputs(output);
        } else {
            internalOutputs = new Output[]{output};
        }
        broadcastOutputs = new BroadcastOutput[internalOutputs.length];
        internalOutputTags = new OutputTag[internalOutputs.length];
        numOutChannels = new int[internalOutputs.length];
        for (int i = 0; i < internalOutputs.length; i++) {
            if (myOutputReflectionContext.isRecordWriterOutput(internalOutputs[i])) {
                OutputTag<?> outputTag = myOutputReflectionContext.getRecordWriterOutputTag(internalOutputs[i]);
                    // This is meant for the next layer
                RecordWriter<SerializationDelegate<StreamElement>> recordWriter =
                        myOutputReflectionContext.getRecordWriter(internalOutputs[i]);
                TypeSerializer<StreamElement> typeSerializer =
                        myOutputReflectionContext.getRecordWriterTypeSerializer(internalOutputs[i]);

                broadcastOutputs[i] = new RecordWriterBroadcastOutput<>(recordWriter, typeSerializer);
                internalOutputTags[i] = outputTag;
                numOutChannels[i] = myOutputReflectionContext.getNumChannels(internalOutputs[i]);
            } else {
                OutputTag<?> outputTag = myOutputReflectionContext.getChainingOutputTag(internalOutputs[i]);
                broadcastOutputs[i] = myOutputReflectionContext.createChainingBroadcastOutput(internalOutputs[i], outputTag);
                internalOutputTags[i] = outputTag;
                numOutChannels[i] = containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();
            }
        }
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
         * Broadcasts this message to the forward chain, identified with null outputTag
         * All other broadcasts should be done via IterationSource -> Tails that is only sent as regular events
         */
        public <E> void broadcastOutput(E el, @Nullable OutputTag<E> tag, @Nullable Long timestamp) throws IOException {
            // 1. Store the previous value
            Long tmpTs = element.hasTimestamp() ? element.getTimestamp() : null;
            GraphOp tmpVal = element.getValue();
            StreamRecord<E> replaced;
            if (timestamp == null) {
                replaced = element.replace(el);
            } else {
                replaced = element.replace(el, timestamp);
            }
            // 2. Send the output
            for (int i = 0; i < internalOutputTags.length; i++) {
                if(Objects.equals(tag, internalOutputTags[i])){
                    broadcastOutputs[i].broadcastEmit(replaced);
                }
            }
            // 3. Return the previous value
            if (tmpTs != null) {
                element.replace(tmpVal, tmpTs);
            } else {
                element.replace(tmpVal);
                element.eraseTimestamp();
            }
        }

        /**
         * Emits the record with custom timestamp to a different outputTag
         * @implNote if outputTag=null, emit to the next in the pipeline
         * @implNote if timestamp= null, emit with the timestamp of the current elemetn
         * Returns the current timestamp after emitting the event
         */
        public <E> void output(E el, @Nullable OutputTag<E> tag, @Nullable Long timestamp) {
            Long tmpTs = element.hasTimestamp() ? element.getTimestamp() : null;
            GraphOp tmpVal = element.getValue();
            StreamRecord<E> replaced;
            if (timestamp == null) {
                replaced = element.replace(el);
            } else {
                replaced = element.replace(el, timestamp);
            }
            // 2. Send the output
            if(tag == null){
                output.collect((StreamRecord<GraphOp>) replaced);
            }else{
                output.collect(tag, replaced);
            }
            // 3. Return the previous value
            if (tmpTs != null) {
                element.replace(tmpVal, tmpTs);
            } else {
                element.replace(tmpVal);
                element.eraseTimestamp();
            }
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
         */
        public List<Short> getOtherOperatorMasterParts() {
            return otherMasterParts;
        }

        /**
         * Run the Runnable on all parts that are hashed to this operator
         * @param o Runnable Function
         */
        public void runForAllKeys(Runnable o) throws Exception {
            Short tmp = element.getValue().getPartId();
            for (Short thisPart : thisParts) {
                element.getValue().setPartId(thisPart);
                setKeyContextElement(element);
                o.run();
            }
            element.getValue().setPartId(tmp);
            setKeyContextElement(element);
        }

        /**
         * Registeringey change listener
         */
        public void registerKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener){
            getWrappedOperator().getKeyedStateBackend().registerKeySelectionListener(listener);
        }

        /**
         * De-Registeringey change listener
         */
        public void deRegisterKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener){
            getWrappedOperator().getKeyedStateBackend().deregisterKeySelectionListener(listener);
        }
    }

}
