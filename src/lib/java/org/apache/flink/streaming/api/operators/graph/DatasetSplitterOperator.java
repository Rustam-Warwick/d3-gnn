package org.apache.flink.streaming.api.operators.graph;

import elements.GraphElement;
import elements.GraphEvent;
import elements.GraphOp;
import elements.Plugin;
import elements.enums.Op;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
import org.apache.flink.runtime.state.taskshared.TaskSharedState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.CountingBroadcastingGraphOutputCollector;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import storage.GraphStorage;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * <strong>SPLITTER</strong> operator located at the start of the operator chain
 * <p>
 * Apart from taking in the {@link KeyedProcessFunction} for splitting the {@link datasets.Dataset}
 * also handles the training and flushing the input stream.
 * On reception of {@link TrainingSubCoordinator.FlushForTraining} it stops the operator from consuming messages
 * This causes a backpressure which also hold the checkpoints. Note that iteration messages will still flow normally only topological messages will stop
 * On reception of {@link org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator.ResumeInference} it will resume the computation and continue streaming
 * </p>
 *
 * @implNote Always located at position 0, hence no explicit position passed in here
 */
public class DatasetSplitterOperator extends KeyedProcessOperator<PartNumber, GraphOp, GraphOp> implements ExposingInternalTimerService, OperatorEventHandler {

    /**
     * Reference to the {@link Output} for this operator but type cast into {@link CountingBroadcastingGraphOutputCollector}
     */
    protected final CountingBroadcastingGraphOutputCollector thisOutput;

    /**
     * Number of layers in the GNN pipeline
     */
    protected final short layers;

    /**
     * Reuse element for handling timestamps and performance reasons
     */
    protected final StreamRecord<GraphOp> reuse = new StreamRecord<>(null);

    /**
     * Operator Event Gateway
     */
    protected final OperatorEventGateway operatorEventGateway;

    /**
     * Internal Timer Service
     */
    protected InternalTimerService<VoidNamespace> internalTimerService;

    /**
     * Mailbox executor for handling operator events in case training mod
     */
    protected final MailboxExecutor mailboxExecutor;

    /**
     * Operation mode of the splitter
     */
    protected OperationMode operationMode = OperationMode.RUNNING;

    /**
     * GraphEvent pool
     */
    protected final GraphEventPool eventPool;

    public DatasetSplitterOperator(short layers, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> function, ProcessingTimeService processingTimeService, MailboxExecutor mailboxExecutor, StreamOperatorParameters<GraphOp> parameters) {
        super(function);
        this.processingTimeService = processingTimeService;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        this.layers = layers;
        this.mailboxExecutor = mailboxExecutor;
        this.output = this.thisOutput = new CountingBroadcastingGraphOutputCollector(parameters.getOutput(), getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter());
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        new GraphRuntimeContextImpl();
        this.eventPool = new GraphEventPool(this);
        parameters.getOperatorEventDispatcher().registerEventHandler(getOperatorID(), this);
    }

    @Override
    public void open() throws Exception {
        super.open();
        internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.hasTimestamp()) reuse.setTimestamp(element.getTimestamp());
        else reuse.eraseTimestamp();
        if(element.getValue().op == Op.OPERATOR_EVENT) eventPool.addEvent(element.getValue().graphEvent);
        else super.processElement(element);
    }

    @Override
    public <K> TaskSharedKeyedStateBackend<K> getKeyedStateBackend() {
        return (TaskSharedKeyedStateBackend<K>) super.getKeyedStateBackend(); // Typecast error if not
    }

    @Override
    public InternalTimerService<VoidNamespace> getInternalTimerService() {
        return internalTimerService;
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        if(evt instanceof TrainingSubCoordinator.FlushForTraining){
            thisOutput.broadcastAll(reuse.replace(new GraphOp((GraphEvent) evt)));
            operationMode = OperationMode.TRAINING;
            try{
                while(operationMode == OperationMode.TRAINING){
                    mailboxExecutor.yield();
                }
            }catch (InterruptedException ignored){
                // Can be interrupted to close prematurely
            }
        }
        else if(evt instanceof TrainingSubCoordinator.ResumeInference){
            // Back to running mode
            operationMode = OperationMode.RUNNING;
            System.out.println("STARTING");
        }
    }

    /**
     * Mode of operation of this operator
     */
    enum OperationMode{
        RUNNING,
        TRAINING
    }

    public class GraphRuntimeContextImpl extends GraphRuntimeContext {

        @Override
        public GraphStorage.GraphStorageView getStorage() {
            throw new IllegalStateException("No storage in SPLITTER");
        }

        @Override
        public Plugin getPlugin(String pluginId) {
            throw new IllegalStateException("No plugin in SPLITTER");
        }

        @Override
        public void output(GraphOp op) {
            thisOutput.collect(reuse.replace(op));
        }

        @Override
        public void output(GraphOp op, OutputTag<GraphOp> tag) {
            thisOutput.collect(tag, reuse.replace(op));
        }

        @Override
        public <T> void output(T el, OutputTag<T> tag) {
            thisOutput.collect(tag, reuse.replace(el));
        }

        @Override
        public void broadcastAll(GraphOp op) {
            thisOutput.broadcastAll(reuse.replace(op));
        }

        @Override
        public void broadcast(GraphOp op) {
            thisOutput.broadcast(null,reuse.replace(op));
        }

        @Override
        public void broadcast(GraphOp op, OutputTag<GraphOp> tag) {
            thisOutput.broadcast(tag, reuse.replace(op));
        }

        @Override
        public void broadcast(GraphOp op, List<Short> selectedPartsOnly) {
            thisOutput.broadcast(null, reuse.replace(op), selectedPartsOnly);
        }

        @Override
        public void broadcast(GraphOp op, OutputTag<GraphOp> tag, List<Short> selectedPartsOnly) {
            thisOutput.broadcast(tag, reuse.replace(op), selectedPartsOnly);
        }

        @Override
        public int getNumOfOutChannels() {
            return thisOutput.getNumChannels(null);
        }

        @Override
        public int getNumOfOutChannels(OutputTag<GraphOp> tag) {
            return thisOutput.getNumChannels(tag);
        }

        @Override
        public void runForAllLocalParts(Runnable run) {
            PartNumber initialKey = (PartNumber) getCurrentKey();
            try{
                for (short thisOperatorPart : getThisOperatorParts()) {
                    setCurrentKey(PartNumber.of(thisOperatorPart));
                    run.run();
                }
            }finally {
                setCurrentKey(initialKey);
            }
        }

        @Override
        public TimerService getTimerService() {
            throw new IllegalStateException("Not in SPLITTER");
        }

        @Override
        public IndexedInputGate[] getInputGates() {
            return getContainingTask().getEnvironment().getAllInputGates();
        }

        @Override
        public short getPosition() {
            return 0;
        }

        @Override
        public short getLayers() {
            return layers;
        }

        @Override
        public short getCurrentPart() {
            return ((PartNumber) getCurrentKey()).partId;
        }

        @Override
        public TaskSharedKeyedStateBackend<PartNumber> getKeyedStateBackend() {
            return DatasetSplitterOperator.this.getKeyedStateBackend();
        }

        @Override
        public OperatorEventGateway getOperatorEventGateway() {
            return operatorEventGateway;
        }

        @Override
        public long currentTimestamp() {
            return reuse.getTimestamp();
        }

        @Override
        public void addElementCallback(GraphElement element) {
            throw new IllegalStateException("Not in SPLITTER");
        }

        @Override
        public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
            throw new IllegalStateException("Not in SPLITTER");
        }

        @Override
        public void deleteElementCallback(GraphElement deletedElement) {
            throw new IllegalStateException("Not in SPLITTER");
        }

        @Override
        public JobID getJobId() {
            return getRuntimeContext().getJobId();
        }

        @Override
        public String getTaskName() {
            return getRuntimeContext().getTaskName();
        }

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return getRuntimeContext().getMetricGroup();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return getRuntimeContext().getNumberOfParallelSubtasks();
        }

        @Override
        public int getMaxNumberOfParallelSubtasks() {
            return getRuntimeContext().getMaxNumberOfParallelSubtasks();
        }

        @Override
        public int getIndexOfThisSubtask() {
            return getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public int getAttemptNumber() {
            return getRuntimeContext().getAttemptNumber();
        }

        @Override
        public String getTaskNameWithSubtasks() {
            return getRuntimeContext().getTaskNameWithSubtasks();
        }

        @Override
        public ExecutionConfig getExecutionConfig() {
            return getRuntimeContext().getExecutionConfig();
        }

        @Override
        public ClassLoader getUserCodeClassLoader() {
            return getRuntimeContext().getUserCodeClassLoader();
        }

        @Override
        public void registerUserCodeClassLoaderReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
            getRuntimeContext().registerUserCodeClassLoaderReleaseHookIfAbsent(releaseHookName, releaseHook);
        }

        @Override
        public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
            getRuntimeContext().addAccumulator(name, accumulator);
        }

        @Override
        public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
            return getRuntimeContext().getAccumulator(name);
        }

        @Override
        public IntCounter getIntCounter(String name) {
            return getRuntimeContext().getIntCounter(name);
        }

        @Override
        public LongCounter getLongCounter(String name) {
            return getRuntimeContext().getLongCounter(name);
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            return getRuntimeContext().getDoubleCounter(name);
        }

        @Override
        public Histogram getHistogram(String name) {
            return getRuntimeContext().getHistogram(name);
        }

        @Override
        public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
            return getRuntimeContext().getExternalResourceInfos(resourceName);
        }

        @Override
        public boolean hasBroadcastVariable(String name) {
            return getRuntimeContext().hasBroadcastVariable(name);
        }

        @Override
        public <RT> List<RT> getBroadcastVariable(String name) {
            return getRuntimeContext().getBroadcastVariable(name);
        }

        @Override
        public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
            return getRuntimeContext().getBroadcastVariableWithInitializer(name, initializer);
        }

        @Override
        public DistributedCache getDistributedCache() {
            return getRuntimeContext().getDistributedCache();
        }

        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            return getRuntimeContext().getState(stateProperties);
        }

        @Override
        public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
            return getRuntimeContext().getListState(stateProperties);
        }

        @Override
        public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
            return getRuntimeContext().getReducingState(stateProperties);
        }

        @Override
        public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
            return getAggregatingState(stateProperties);
        }

        @Override
        public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
            return getRuntimeContext().getMapState(stateProperties);
        }

        @Override
        public <S extends TaskSharedState> S getTaskSharedState(TaskSharedStateDescriptor<S, ?> taskSharedStateDescriptor) {
            return getKeyedStateBackend().getOrCreateTaskSharedState(VoidNamespace.get(), VoidNamespaceSerializer.INSTANCE, taskSharedStateDescriptor);
        }
    }

}
