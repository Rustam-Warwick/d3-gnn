package org.apache.flink.streaming.api.operators.graph;

import elements.GraphElement;
import elements.GraphOp;
import elements.Plugin;
import elements.Rmi;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.tmshared.TMSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.tmshared.TMSharedKeyedStateBackend;
import org.apache.flink.runtime.state.tmshared.TMSharedState;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.CountingBroadcastingGraphOutputCollector;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Graph Storage operator that contains multiple {@link Plugin} and a {@link BaseStorage}
 * Assumes Partitioned {@link GraphOp} as input and output
 */
public class GraphStorageOperator extends AbstractStreamOperator<GraphOp> implements OneInputStreamOperator<GraphOp, GraphOp>, Triggerable<PartNumber, VoidNamespace>, OperatorEventHandler, ExposingInternalTimerService {

    /**
     * Storage constructor access for state creation
     */
    protected final BaseStorage.GraphStorageProvider storageProvider;

    /**
     * This horizontal position of this pipeline
     */
    protected final short position;

    /**
     * Number of layers in the pipeline
     */
    protected final short layers;

    /**
     * Reuse element for handling timestamps and performance reasons
     */
    protected final StreamRecord<GraphOp> reuse = new StreamRecord<>(null);

    /**
     * Reference to the {@link Output} for this operator but type cast into {@link CountingBroadcastingGraphOutputCollector}
     */
    protected final CountingBroadcastingGraphOutputCollector thisOutput;

    /**
     * Operator Event Gateway
     */
    protected final OperatorEventGateway operatorEventGateway;

    /**
     * Event pool for handling flowing {@link elements.GraphEvent}
     */
    protected final GraphEventPool eventPool;

    /**
     * Map of ID -> Plugin. Actually {@link TMSharedGraphPerPartMapState}
     */
    protected final Map<String, Plugin> plugins;

    /**
     * Graph Runtime context
     */
    protected final GraphRuntimeContext graphRuntimeContext;

    /**
     * Operator IO metric
     */
    protected final OperatorIOMetricGroup operatorIOMetricGroup;

    /**
     * Storage where all the {@link GraphElement} are stored. Except for {@link Plugin}
     */
    protected BaseStorage.GraphView storage;

    /**
     * Internal Timer Service
     */
    protected InternalTimerService<VoidNamespace> internalTimerService;

    /**
     * User time service that {@link GraphElement} interact with
     */
    protected TimerService userTimerService;

    /**
     * Counter for flush detection
     */
    protected long pipelineFlushingCounter = 0;

    public GraphStorageOperator(List<Plugin> plugins, short position, short layers, BaseStorage.GraphStorageProvider storageProvider, ProcessingTimeService processingTimeService, StreamOperatorParameters<GraphOp> parameters) {
        this.processingTimeService = processingTimeService;
        super.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        this.operatorIOMetricGroup = getMetricGroup().getIOMetricGroup();
        this.storageProvider = storageProvider;
        this.position = position;
        this.layers = layers;
        this.graphRuntimeContext = new GraphRuntimeContextImpl(); // Create so that it gets stored to threadLocal
        this.plugins = new LinkedHashMap<>(plugins.stream().collect(Collectors.toMap(Plugin::getId, p -> p)));
        this.plugins.values().forEach(plugin -> plugin.setRuntimeContext(this.graphRuntimeContext));
        this.output = this.thisOutput = new CountingBroadcastingGraphOutputCollector(parameters.getOutput(), operatorIOMetricGroup.getNumRecordsOutCounter());
        this.eventPool = new GraphEventPool(this, this.graphRuntimeContext);
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        parameters.getOperatorEventDispatcher().registerEventHandler(getOperatorID(), this);
    }

    @Override
    public void open() throws Exception {
        super.open();
        internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
        userTimerService = new SimpleTimerService(internalTimerService);
        Configuration tmp = new Configuration();
        for (Plugin plugin : plugins.values()) {
            plugin.open(tmp);
        }
        getRuntimeContext().getMetricGroup().gauge("tensors_mb", storage::tensorMemoryUsageInMb);
    }

    @Override
    public void close() throws Exception {
        for (Plugin plugin : plugins.values()) {
            plugin.close();
        }
        super.close();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.storage = graphRuntimeContext.getTaskSharedState(new TMSharedStateDescriptor<>("storage", TypeInformation.of(BaseStorage.class), storageProvider)).getGraphStorageView(this.graphRuntimeContext);
        for (Plugin plugin : plugins.values()) {
            plugin.initializeState(context);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        for (Plugin plugin : plugins.values()) {
            plugin.snapshotState(context);
        }
    }

    @Override
    public void onEventTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {
        reuse.setTimestamp(timer.getTimestamp());
        for (Plugin plugin : plugins.values()) {
            plugin.onEventTime(timer);
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {
        reuse.eraseTimestamp();
        for (Plugin plugin : plugins.values()) {
            plugin.onProcessingTime(timer);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        for (Plugin value : plugins.values()) {
            value.updateCurrentEffectiveWatermark(mark.getTimestamp());
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        if (evt instanceof TrainingSubCoordinator.FlushingScanRequest) {
            long tmp = operatorIOMetricGroup.getNumRecordsInCounter().getCount() + operatorIOMetricGroup.getNumRecordsOutCounter().getCount();
            final boolean[] hasTimers = new boolean[]{false};
            try {
                getInternalTimerService().forEachProcessingTimeTimer((ns, timer) -> {
                    hasTimers[0] = true;
                    throw new Exception("Found, do not process rest");
                });
            } catch (Exception ignored) {
            }
            operatorEventGateway.sendEventToCoordinator(new TrainingSubCoordinator.FlushingScanResponse(!hasTimers[0] && tmp == pipelineFlushingCounter));
            pipelineFlushingCounter = tmp;
        }
        for (Plugin plugin : plugins.values()) {
            plugin.handleOperatorEvent(evt);
        }
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.hasTimestamp()) reuse.setTimestamp(element.getTimestamp());
        else reuse.eraseTimestamp();
        GraphOp value = element.getValue();
        try (BaseStorage.ObjectPoolScope ignored = storage.openObjectPoolScope()) {
            switch (value.op) {
                case ADD:
                    value.element.create();
                    break;
                case COMMIT:
                    if (!storage.containsElement(value.element)) {
                        value.element.create();
                    } else {
                        storage.getElement(value.element).update(value.element);
                    }
                    break;
                case SYNC_REQUEST:
                    if (!storage.containsElement(value.element.getId(), value.element.getType())) {
                        // This can only occur if master is not here yet
                        GraphElement el = storage.getDummyElementAsMaster(value.element.getId(), value.element.getType());
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
                    eventPool.addEvent(element.getValue().graphEvent);
                    break;
            }
        } catch (Exception e) {
            LOG.error(org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public InternalTimerService<VoidNamespace> getInternalTimerService() {
        return internalTimerService;
    }

    @Override
    public <K> TMSharedKeyedStateBackend<K> getKeyedStateBackend() {
        return (TMSharedKeyedStateBackend<K>) super.getKeyedStateBackend(); // Typecast error if not
    }

    public class GraphRuntimeContextImpl extends GraphRuntimeContext {

        @Override
        public BaseStorage.GraphView getStorage() {
            return storage;
        }

        @Override
        public Plugin getPlugin(String pluginId) {
            return plugins.get(pluginId);
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
            thisOutput.broadcast(null, reuse.replace(op));
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
            try {
                for (short thisOperatorPart : getThisOperatorParts()) {
                    setCurrentKey(PartNumber.of(thisOperatorPart));
                    run.run();
                }
            } finally {
                setCurrentKey(initialKey);
            }
        }

        @Override
        public void runWithTimestamp(Runnable run, long ts) {
            Long oldTs = reuse.hasTimestamp() ? reuse.getTimestamp() : null;
            reuse.setTimestamp(ts);
            try {
                run.run();
            } finally {
                if (oldTs == null) reuse.eraseTimestamp();
                else reuse.setTimestamp(oldTs);
            }
        }

        @Override
        public TimerService getTimerService() {
            return userTimerService;
        }

        @Override
        public IndexedInputGate[] getInputGates() {
            return getContainingTask().getEnvironment().getAllInputGates();
        }

        @Override
        public short getPosition() {
            return position;
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
        public TMSharedKeyedStateBackend<PartNumber> getKeyedStateBackend() {
            return GraphStorageOperator.this.getKeyedStateBackend();
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
            plugins.values().forEach(plugin -> {
                if (plugin.isListening()) plugin.addElementCallback(element);
            });
        }

        @Override
        public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
            plugins.values().forEach(plugin -> {
                if (plugin.isListening()) plugin.updateElementCallback(newElement, oldElement);
            });
        }

        @Override
        public void deleteElementCallback(GraphElement deletedElement) {
            plugins.values().forEach(plugin -> {
                if (plugin.isListening()) plugin.deleteElementCallback(deletedElement);
            });
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
        public <S extends TMSharedState> S getTaskSharedState(TMSharedStateDescriptor<S, ?> TMSharedStateDescriptor) {
            return getKeyedStateBackend().getOrCreateTaskSharedState(VoidNamespace.get(), VoidNamespaceSerializer.INSTANCE, TMSharedStateDescriptor);
        }
    }

}
