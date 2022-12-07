package operators;

import elements.GraphElement;
import elements.GraphOp;
import elements.Plugin;
import elements.Rmi;
import operators.interfaces.GraphRuntimeContext;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Graph Storage operator that contains {@link Plugin} and {@link BaseStorage}
 */
public class GraphStorageOperator extends AbstractStreamOperator<GraphOp> implements OneInputStreamOperator<GraphOp, GraphOp> {

    protected Map<String, Plugin> plugins;

    protected BaseStorage storage;

    public GraphStorageOperator(List<Plugin> plugins, BaseStorage graphStorage, StreamOperatorParameters<GraphOp> parameters) {
        this.processingTimeService = parameters.getProcessingTimeService();
        this.storage = graphStorage;
        this.plugins = new HashMap<>();
        plugins.forEach(plugin -> this.plugins.put(plugin.getId(), plugin));
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        GraphRuntimeContext impl = new GraphRuntimeContextImpl();
        storage.setRuntimeContext(impl);
        this.plugins.values().forEach(plugin -> plugin.setRuntimeContext(impl));
    }

    @Override
    public void open() throws Exception {
        super.open();
        Configuration tmp = new Configuration();
        storage.open(tmp);
        for (Plugin value : plugins.values()) {
            value.open(tmp);
        }
    }

    @Override
    public void close() throws Exception {
        storage.close();
        for (Plugin value : plugins.values()) {
            value.close();
        }
        super.close();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        storage.initializeState(context);
        for (Plugin value : plugins.values()) {
            value.initializeState(context);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        storage.snapshotState(context);
        for (Plugin value : plugins.values()) {
            value.snapshotState(context);
        }
    }



    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        GraphOp value = element.getValue();
        switch (value.op) {
            case COMMIT:
                if (!storage.containsElement(value.element)) {
                    value.element.create();
                } else {
                    storage.getElement(value.element).update(value.element);
                }
                break;
            case SYNC_REQUEST:
                if (storage.containsElement(value.element.getId(), value.element.getType())) {
                    // This can only occur if master is not here yet
                    GraphElement el = storage.getDummyElement(value.element.getId(), value.element.getType());
                    el.create();
                    el.syncRequest(value.element);
                } else {
                    GraphElement el = storage.getElement(value.element.getId(), value.element.getType());
                    el.syncRequest(value.element);
                }
                break;
            case SYNC:
                GraphElement el = storage.getElement(value.element);
                el.sync(value.element);
                break;
            case RMI:
                GraphElement rpcElement = storage.getElement(value.element.getId(), value.element.getType());
                Rmi rmi = (Rmi) value.element;
                Rmi.execute(rpcElement, rmi.methodName, rmi.args);
                break;
            case OPERATOR_EVENT:
                storage.handleOperatorEvent(value.operatorEvent);
                break;
        }
    }


    public class GraphRuntimeContextImpl implements GraphRuntimeContext{
        public GraphRuntimeContextImpl() {
            GraphRuntimeContext.CONTEXT_THREAD_LOCAL.set(this);
        }

        @Override
        public BaseStorage getStorage() {
            return storage;
        }

        @Override
        public Plugin getPlugin(String pluginId) {
            return plugins.get(pluginId);
        }

        @Override
        public void message(GraphOp op) {

        }

        @Override
        public void message(GraphOp op, OutputTag<GraphOp> tag) {

        }

        @Override
        public void broadcastMessage(GraphOp op) {

        }

        @Override
        public void broadcastMessage(GraphOp op, OutputTag<GraphOp> tag) {

        }

        @Override
        public void broadcastMessage(GraphOp op, short... selectedPartsOnly) {

        }

        @Override
        public void broadcastMessage(GraphOp op, OutputTag<GraphOp> tag, short... selectedPartsOnly) {

        }

        @Override
        public void runForAllLocalParts(Runnable run) {

        }

        @Override
        public TimerService getTimerService() {
            return null;
        }

        @Override
        public int getPosition() {
            return 0;
        }

        @Override
        public short getCurrentPart() {
            return ((PartNumber)getCurrentKey()).partId;
        }

        @Override
        public long currentTimestamp() {
            return 0;
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
    }

}
