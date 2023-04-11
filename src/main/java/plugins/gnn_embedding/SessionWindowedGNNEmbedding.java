package plugins.gnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.enums.ElementType;
import elements.enums.Op;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.tmshared.TMSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import storage.BaseStorage;

import java.util.List;
import java.util.Map;

/**
 * Plugin that compute the session-window based GNN for source based messages
 * Windowing happens depth-wise (forward) and breadth-wise (reduce) messages
 */
public class SessionWindowedGNNEmbedding extends StreamingGNNEmbedding {

    private static final double TIMER_COALESCING = 25;
    public final int sessionIntervalMs;
    protected transient Map<Short, Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>>> deepMaps; // eviction times and timestamps
    protected transient Map<Short, Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>>> breadthMaps;  // Eviction times, in-edges, timestamps
    protected transient NDList reuseAggregatorsNDList;
    protected transient ObjectArrayList<String> reuseVertexIdList;
    protected transient Object2IntOpenHashMap<String> reuseVertex2IndexMap;
    protected transient IntArrayList reuseIndexList;

    public SessionWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int sessionIntervalMs) {
        super(modelName, trainableVertexEmbeddings);
        this.sessionIntervalMs = sessionIntervalMs;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseAggregatorsNDList = new NDList();
        reuseIndexList = new IntArrayList();
        reuseVertexIdList = new ObjectArrayList<>();
        reuseVertex2IndexMap = new Object2IntOpenHashMap<>();
        deepMaps = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_deep_maps", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        synchronized (deepMaps){
            getRuntimeContext().getThisOperatorParts().forEach(part -> deepMaps.put(part, Tuple2.of(new Object2LongLinkedOpenHashMap<>(), new Object2LongOpenHashMap<>())));
        }
        breadthMaps = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_breadth_maps", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        synchronized (breadthMaps){
            getRuntimeContext().getThisOperatorParts().forEach(part -> breadthMaps.put(part, Tuple3.of(new Object2LongLinkedOpenHashMap<>(), new Object2ObjectOpenHashMap<>(), new Object2LongOpenHashMap<>())));
        }
    }

    /**
     * {@inheritDoc}
     * If this is a new edge, add to timer queue
     */
    @Override
    public void addElementCallback(GraphElement element) {
        if(element.getType() == ElementType.EDGE){
            DirectedEdge directedEdge = (DirectedEdge) element;
            if (directedEdge.getSrc().containsFeature("f")) {
                reduceTimer(directedEdge, getRuntimeContext().getTimerService().currentProcessingTime() + sessionIntervalMs);
            }
            return;
        }
        super.addElementCallback(element);
    }

    /**
     * {@inheritDoc}
     * Adds the forward messages to timer queue and does not immediately forward it to the next layer
     */
    public void forward(Vertex v) {
        forwardTimer(v, getRuntimeContext().getTimerService().currentProcessingTime() + sessionIntervalMs);
    }

    /**
     * Add the given edge to timer queue to be evicted at updateTime
     */
    public void reduceTimer(DirectedEdge directedEdge, long updateTime){
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        getRuntimeContext().getStorage().deleteEdge(directedEdge);
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partReduceMaps = breadthMaps.get(getRuntimeContext().getCurrentPart());
        partReduceMaps.f0.removeLong(directedEdge.getDestId());
        partReduceMaps.f0.put(directedEdge.getDestId(), updateTime);
        partReduceMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore)->new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partReduceMaps.f2.put(directedEdge.getDestId(), getRuntimeContext().currentTimestamp());
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }

    /**
     * Add the given vertex to timer queue to be evicted at updateTime
     */
    public void forwardTimer(Vertex v, long updateTime){
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = deepMaps.get(getPart());
        maps.f0.removeLong(v.getId());
        maps.f0.put(v.getId(), updateTime);
        maps.f1.put(v.getId(), getRuntimeContext().currentTimestamp());
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }

    /**
     * Evict waiting reduce messages less than the current timestamp
     */
    public void evictReduceUntil(long timestamp){
        try (BaseStorage.ObjectPoolScope objectPool = getRuntimeContext().getStorage().openObjectPoolScope()) {
            // 1. Set placeholders
            final short currentPart = getPart();
            Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> maps = breadthMaps.get(currentPart);
            reuseVertexIdList.clear();
            reuseFeaturesNDList.clear();
            reuseIndexList.clear();
            reuseVertex2IndexMap.clear();

            // 2. Collect data
            ObjectBidirectionalIterator<Object2LongMap.Entry<String>> iterator = maps.f0.object2LongEntrySet().iterator();
            while (iterator.hasNext()) {
                Object2LongMap.Entry<String> vertexTimerEntry = iterator.next();
                if (vertexTimerEntry.getLongValue() > timestamp) {
                    break;
                }
                reuseVertexIdList.add(vertexTimerEntry.getKey());
                List<String> inEdges = maps.f1.remove(vertexTimerEntry.getKey());
                for (String inVertexId : inEdges) {
                    getRuntimeContext().getStorage().addEdge(new DirectedEdge(inVertexId, vertexTimerEntry.getKey(), null));
                    int indexOfSrc = reuseVertex2IndexMap.computeIfAbsent(inVertexId, (key) -> {
                        reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, key, "f").getValue());
                        return reuseFeaturesNDList.size() - 1;
                    });
                    reuseIndexList.add(indexOfSrc);
                    objectPool.refresh();
                }
                reuseIndexList.add(-1);
                iterator.remove();
            }

            // 3. Partial aggregate
            if (reuseFeaturesNDList.isEmpty()) return;
            NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
            reuseFeaturesNDList.clear();
            reuseFeaturesNDList.add(batchedFeatures);
            NDArray batchedSrcMessages = MESSAGE(reuseFeaturesNDList, false).get(0);

            // 4. Send
            int destCount = 0;
            int start = 0;
            final int[] dim = new int[]{0};
            for (int i = 0; i < reuseIndexList.size(); i++) {
                if (reuseIndexList.getInt(i) == -1) {
                    int[] indices = new int[i - start];
                    reuseFeaturesNDList.clear();
                    System.arraycopy(reuseIndexList.elements(), start, indices, 0, indices.length);
                    reuseFeaturesNDList.add(batchedSrcMessages.get(BaseNDManager.getManager().create(indices)).sum(dim));
                    reuseAggId.f1 = reuseVertexIdList.get(destCount++);
                    getRuntimeContext().runWithTimestamp(() -> {
                        Rmi.buildAndRun(
                                reuseAggId,
                                ElementType.ATTACHED_FEATURE,
                                "reduce",
                                getRuntimeContext().getStorage().getVertex((String) reuseAggId.f1).getMasterPart(),
                                OutputTags.ITERATE_OUTPUT_TAG,
                                reuseFeaturesNDList,
                                indices.length
                        );
                    }, maps.f2.removeLong(reuseAggId.f1));
                    objectPool.refresh();
                    start = i + 1;
                }
            }
        }
    }

    /**
     * Evict the waiting forward messages less than the current timestamp
     */
    public void evictForwardUntil(long timestamp) {

        // 1. Set placeholders
        final short currentPart = getPart();
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = deepMaps.get(getPart());
        reuseFeaturesNDList.clear();
        reuseAggregatorsNDList.clear();
        reuseVertexIdList.clear();

        // 2. Collect data
        try (BaseStorage.ObjectPoolScope objectPool = getRuntimeContext().getStorage().openObjectPoolScope()) {
            ObjectBidirectionalIterator<Object2LongMap.Entry<String>> iterator = maps.f0.object2LongEntrySet().iterator();
            while (iterator.hasNext()) {
                Object2LongMap.Entry<String> vertexTimerEntry = iterator.next();
                if (vertexTimerEntry.getLongValue() > timestamp) {
                    break;
                }
                Vertex v = getRuntimeContext().getStorage().getVertex(vertexTimerEntry.getKey());
                reuseFeaturesNDList.add((NDArray) (v.getFeature("f")).getValue());
                reuseAggregatorsNDList.add((NDArray) (v.getFeature("agg")).getValue());
                reuseVertexIdList.add(v.getId());
                iterator.remove();
                objectPool.refresh();
            }
        }

        // 3. Forward
        if (reuseVertexIdList.isEmpty()) return;
        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        NDArray batchedAggregators = NDArrays.stack(reuseAggregatorsNDList);
        reuseFeaturesNDList.clear();
        reuseFeaturesNDList.add(batchedFeatures);
        reuseFeaturesNDList.add(batchedAggregators);
        NDArray batchedUpdates = UPDATE(reuseFeaturesNDList, false).get(0);

        // 4. Send messages
        throughput.inc(reuseVertexIdList.size());
        for (int i = 0; i < reuseVertexIdList.size(); i++) {
            reuseTensor.value = batchedUpdates.get(i);
            reuseTensor.id.f1 = reuseVertexIdList.get(i);
            getRuntimeContext().runWithTimestamp(() -> {
                        getRuntimeContext().output(new GraphOp(Op.COMMIT, currentPart, reuseTensor));
                        latency.inc(getRuntimeContext().getTimerService().currentProcessingTime() - getRuntimeContext().currentTimestamp());
                    },
                    maps.f1.removeLong(reuseVertexIdList.get(i)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onProcessingTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {
        super.onProcessingTime(timer);
        evictReduceUntil(timer.getTimestamp());
        evictForwardUntil(timer.getTimestamp());
    }

}
