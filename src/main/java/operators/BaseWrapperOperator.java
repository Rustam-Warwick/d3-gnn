package operators;


import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.GraphOp;
import elements.enums.Op;
import elements.enums.MessageCommunication;
import elements.enums.MessageDirection;
import helpers.MyOutputReflectionContext;
import operators.events.BaseOperatorEvent;
import operators.events.FinalWatermarkArrived;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.RecordWriterBroadcastOutput;
import org.apache.flink.iteration.operator.OperatorUtils;
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
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Operator that Wraps around another operator and implements some common logic
 *
 * @implNote Assumes that the input is GraphOp
 * @implNote Assumes that if the input is keyed it should be KeyedBy PartNumber
 * @see GNNLayerWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends AbstractStreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, Input<GraphOp>, OperatorEventHandler, StreamOperatorStateHandler.CheckpointedStreamOperator, FeedbackConsumer<StreamRecord<GraphOp>> {

    /**
     * STATIC PROPS
     */

    public static final Logger LOG = LoggerFactory.getLogger(BaseWrapperOperator.class);

    private static final OutputTag<GraphOp> FORWARD_OUTPUT_TAG = new OutputTag<>("forward", TypeInformation.of(GraphOp.class)); // used to retrive forward output, since hashmap cannot have null values

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

    protected transient Map<OutputTag<?>, Tuple2<BroadcastOutput<?>, Integer>> broadcastOutputs;
    protected transient int numPreviousLayerInputChannels; // Forwarded from previous layer
    protected transient List<Short> thisParts; // Part Keys hashed to this operator, first one is regarded MASTER key. Used in broadcast outputs
    protected transient List<Short> otherMasterParts; // Master Parts that are mapped to other operators

    protected transient Map<BaseOperatorEvent, Short> events; // Table of FlowingOperatorEvents received by this operator
    private transient MailboxExecutor mailboxExecutor; // Mailbox for consuming iteration events
    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

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
        this.broadcastOutputs = new HashMap<>(5);
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(getOperatorID(), this);
        createInputAndOutputs(output);
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
        System.gc();
        System.runFinalization();
    }

    @Override
    public void finish() throws Exception {
        feedbackChannel.getPhaser().awaitAdvanceInterruptibly(feedbackChannel.getPhaser().arrive());
        do {
            Thread.sleep(5000);
        } while (mailboxExecutor.tryYield() || feedbackChannel.getTotalFlowingMessageCount() > 0);
        wrappedOperator.finish();
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(feedbackChannel);
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
        context.element = record;
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
        feedbackChannel.addSnapshot(checkpointId);
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
        mailboxExecutor = containingTask.getMailboxExecutorFactory().createExecutor(TaskMailbox.MAX_PRIORITY);
        registerFeedbackConsumer((Runnable task) -> mailboxExecutor.execute(task::run, "Feedback"));
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
        if (mark.getTimestamp() == Long.MAX_VALUE) {
            operatorEventGateway.sendEventToCoordinator(new FinalWatermarkArrived());
        }
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
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
            // Need to open context here because out of main mailbox loop
            processElement(context.element.replace(new GraphOp((BaseOperatorEvent) evt)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * If processed element in FlowingOperatorEvent which actually came from the pipeline, need to aggregate its count and trigger only if all broadcasts have been received
     */
    @Override
    public final void processElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getOp() == Op.OPERATOR_EVENT) {
            BaseOperatorEvent event = element.getValue().getOperatorEvent();
            boolean processNow;
            if (event.direction == null) processNow = true;
            else {
                short sum = events.merge(event, (short) 1, (a, b) -> (short) (a + b));
                processNow = ((event.direction == MessageDirection.BACKWARD && sum >= context.getNumberOfOutChannels(null)) || (event.direction == MessageDirection.ITERATE && sum >= context.getNumberOfOutChannels(ITERATE_OUTPUT_TAG)) || (event.direction == MessageDirection.FORWARD && sum >= numPreviousLayerInputChannels));
                if (processNow) events.remove(event);
            }
            if (processNow) {
                element.getValue().setPartId(thisParts.get(0));
                setKeyContextElement(element);
                processActualElement(element);
            }
        } else {
            if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
                element.getValue().setPartId(thisParts.get(0));
                setKeyContextElement(element);
            }
            processActualElement(element);
        }
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
            setKeyContextElement(element);
            processElement(element);
        } finally {
            element.getValue().resume();
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
    public void createInputAndOutputs(
            Output<StreamRecord<GraphOp>> output) {
        numPreviousLayerInputChannels = containingTask.getEnvironment().getAllInputGates()[0].getNumberOfInputChannels(); // First one is the previous layer outputs always
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();
        Output<StreamRecord<Object>>[] internalOutputs;
        if (myOutputReflectionContext.isBroadcastingOutput(output)) {
            internalOutputs =
                    myOutputReflectionContext.getBroadcastingInternalOutputs(output);
        } else {
            internalOutputs = new Output[]{output};
        }
        for (Output<StreamRecord<Object>> internalOutput : internalOutputs) {
            if (myOutputReflectionContext.isRecordWriterOutput(internalOutput)) {
                OutputTag<?> outputTag = myOutputReflectionContext.getRecordWriterOutputTag(internalOutput);
                // This is meant for the next layer
                RecordWriter<SerializationDelegate<StreamElement>> recordWriter =
                        myOutputReflectionContext.getRecordWriter(internalOutput);
                TypeSerializer<StreamElement> typeSerializer =
                        myOutputReflectionContext.getRecordWriterTypeSerializer(internalOutput);
                broadcastOutputs.put(outputTag == null ? FORWARD_OUTPUT_TAG : outputTag, Tuple2.of(new RecordWriterBroadcastOutput<>(recordWriter, typeSerializer), myOutputReflectionContext.getNumChannels(internalOutput)));
            } else {
                OutputTag<?> outputTag = myOutputReflectionContext.getChainingOutputTag(internalOutput);
                broadcastOutputs.put(outputTag == null ? FORWARD_OUTPUT_TAG : outputTag, Tuple2.of(myOutputReflectionContext.createChainingBroadcastOutput(internalOutput, outputTag), containingTask.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks()));
            }
        }
    }

    /**
     * Register the consumer
     */
    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = getWrappedOperator().getRuntimeContext().getIndexOfThisSubtask();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationID, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, getWrappedOperator().getRuntimeContext().getAttemptNumber());
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.feedbackChannel = broker.getChannel(key);
        feedbackChannel.registerConsumer(this, mailboxExecutor);
        feedbackChannel.getPhaser().register();
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
        StreamRecord<GraphOp> element = new StreamRecord<>(new GraphOp(Op.NONE, thisParts.get(0), null));

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
            if (outputTag == null) outputTag = FORWARD_OUTPUT_TAG;
            return broadcastOutputs.get(outputTag).f1;
        }

        /**
         * Broadcasts this message to the output tag. If null simply forward broadcast
         */
        public <E> void broadcastOutput(E el, @Nullable OutputTag<E> tag, @Nullable Long timestamp) {
            // 1. Store the previous value
            if (tag == null) tag = (OutputTag<E>) FORWARD_OUTPUT_TAG;
            Long tmpTs = element.hasTimestamp() ? element.getTimestamp() : null;
            GraphOp tmpVal = element.getValue();
            StreamRecord<E> replaced;
            if (timestamp == null) {
                replaced = element.replace(el);
            } else {
                replaced = element.replace(el, timestamp);
            }
            // 2. Send the output
            BroadcastOutput<E> broadcaster = (BroadcastOutput<E>) broadcastOutputs.get(tag).f0;
            try {
                broadcaster.broadcastEmit(replaced);
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage());
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
         *
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
            if (tag == null) {
                output.collect((StreamRecord<GraphOp>) replaced);
            } else {
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
            return ((PartNumber) getCurrentKey()).partId;
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
         *
         * @param o Runnable Function
         */
        public void runForAllKeys(Runnable o) throws IllegalStateException {
            try {
                Short tmp = element.getValue().getPartId();
                for (Short thisPart : thisParts) {
                    element.getValue().setPartId(thisPart);
                    setKeyContextElement(element);
                    o.run();
                }
                element.getValue().setPartId(tmp);
                setKeyContextElement(element);
            } catch (Exception e) {
                BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e), e);
                throw new IllegalStateException(e.getMessage());
            }
        }

        /**
         * Registeringey change listener
         */
        public void registerKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener) {
            getWrappedOperator().getKeyedStateBackend().registerKeySelectionListener(listener);
        }

        /**
         * De-Registeringey change listener
         */
        public void deRegisterKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener) {
            getWrappedOperator().getKeyedStateBackend().deregisterKeySelectionListener(listener);
        }

        /**
         * Get the current stream record being processed
         */
        public final StreamRecord<GraphOp> getElement() {
            return element;
        }
    }

}
