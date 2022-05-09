package operators;


import elements.GraphOp;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Operator that Wraps around another operators and implements some common logic
 *
 * @implNote This operator is also acting as HeadOperator for the feedback streams
 * @see OneInputUDFWrapperOperator manages wrapping around single operators
 */
abstract public class BaseWrapperOperator<T extends StreamOperator<GraphOp>>
        implements StreamOperator<GraphOp>, FeedbackConsumer<StreamRecord<GraphOp>> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.iteration.operator.AbstractWrapperOperator.class);
    public static OutputTag<GraphOp> iterateOutputTag = new OutputTag<GraphOp>("iterate", TypeInformation.of(GraphOp.class));
    public static OutputTag<GraphOp> backwardOutputTag = new OutputTag<GraphOp>("backward", TypeInformation.of(GraphOp.class));
    protected final StreamOperatorParameters<GraphOp> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final Output<StreamRecord<GraphOp>> output;
    protected final StreamOperatorFactory<GraphOp> operatorFactory;
    protected final T wrappedOperator;
    protected final IterationID iterationId;
    protected final MailboxExecutor mailboxExecutor;
    protected final Context context;
    /**
     * Metric group for the operator.
     */
    protected final InternalOperatorMetricGroup metrics;
    protected final String uniqueSenderId;
    protected Output<StreamRecord<GraphOp>>[] internalOutputs;
    // --------------- proxy ---------------------------

//    protected final ProxyOutput<T> proxyOutput;

    // --------------- Metrics ---------------------------
    protected BroadcastOutput<GraphOp>[] internalBroadcastOutputs;

    // ------------- Iteration Related --------------------
    protected OutputTag<GraphOp>[] internalOutputTags;

    public BaseWrapperOperator(
            StreamOperatorParameters<GraphOp> parameters,
            StreamOperatorFactory<GraphOp> operatorFactory,
            IterationID iterationID) {
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

        this.uniqueSenderId =
                OperatorUtils.getUniqueSenderId(
                        streamConfig.getOperatorID(), containingTask.getIndexInSubtaskGroup());

        try {
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

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {

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
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public void createIndividualOutputs(
            Output<StreamRecord<GraphOp>> output, Counter numRecordsOut) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method createInternalBroadcastOutput = BroadcastOutputFactory.class.getDeclaredMethod("createInternalBroadcastOutput", Output.class, OutputReflectionContext.class);
        createInternalBroadcastOutput.setAccessible(true);
        Output<StreamRecord<GraphOp>>[] rawOutputs;
        BroadcastOutput<GraphOp>[] rawBroadcastOutputs;
        OutputTag<GraphOp>[] rawOutputTags;
        MyOutputReflectionContext myOutputReflectionContext = new MyOutputReflectionContext();

        List<BroadcastOutput<GraphOp>> internalOutputs = new ArrayList<>();
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
     *
     * @param <T>
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
            // Pass because watermarkStatuses should be managed by the wrapper operators
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            output.collect(outputTag, record);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            // Pass because watermarkStatuses should be managed by the wrapper operators
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
        protected void emitWatermark(OutputTag<?> outputTag, Watermark e) {

        }

        protected void emitWatermark(Watermark e) {

        }

        public void broadcastElement(OutputTag<GraphOp> outputTag, GraphOp el) {

        }

        public void broadcastElement(GraphOp el) {

        }

        protected void emitWatermarkStatus(OutputTag<?> outputTag, Watermark e) {

        }

        protected void emitWatermarkStatus(Watermark e) {

        }

        protected void emitLatencyMarker(OutputTag<?> outputTag, LatencyMarker m) {

        }

        protected void emitLatencyMarker(LatencyMarker m) {
        }

    }


}
