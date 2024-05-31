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
 * Base class for the windowed GNN embedding layers
 * <p>
 * Introduces 2 types of windowing:
 * For windowing reduce messages, intra-layer
 * For windowing update message, inter-layer
 * </p>
 *
 * @implNote That the timestamp maps should be naturally-sorted based on eviction times in ascending fashion
 */
public abstract class WindowedGNNEmbedding extends StreamingGNNEmbedding {

    protected static final double TIMER_COALESCING = 5000;

    protected static final int MINI_BATCH_SIZE = 10000;

    protected transient Map<Short, Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>>> interLayerMaps; // eviction times and timestamps
    protected transient Map<Short, Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>>> intraLayerMaps;  // Eviction times, in-edges, timestamps
    protected transient NDList reuseAggregatorsNDList;
    protected transient ObjectArrayList<String> reuseVertexIdList;
    protected transient Object2IntOpenHashMap<String> reuseVertex2IndexMap;
    protected transient IntArrayList reuseIntList;
    protected transient IntArrayList reuseIntList2;

    public WindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, trainableVertexEmbeddings);
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseAggregatorsNDList = new NDList();
        reuseIntList = new IntArrayList();
        reuseIntList2 = new IntArrayList();
        reuseVertexIdList = new ObjectArrayList<>();
        reuseVertex2IndexMap = new Object2IntOpenHashMap<>();
        interLayerMaps = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_interLayer_maps", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        synchronized (interLayerMaps) {
            getRuntimeContext().getThisOperatorParts().forEach(part -> interLayerMaps.put(part, Tuple2.of(new Object2LongLinkedOpenHashMap<>(), new Object2LongOpenHashMap<>())));
        }
        intraLayerMaps = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_intraLayer_maps", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        synchronized (intraLayerMaps) {
            getRuntimeContext().getThisOperatorParts().forEach(part -> intraLayerMaps.put(part, Tuple3.of(new Object2LongLinkedOpenHashMap<>(), new Object2ObjectOpenHashMap<>(), new Object2LongOpenHashMap<>())));
        }
    }

    /**
     * {@inheritDoc}
     * If this is a new edge, add to timer queue
     */
    @Override
    public void addElementCallback(GraphElement element) {
        if (element.getType() == ElementType.EDGE) {
            DirectedEdge directedEdge = (DirectedEdge) element;
            if (directedEdge.getSrc().containsFeature("f")) {
                getRuntimeContext().getStorage().deleteEdge(directedEdge);
                intraLayerWindow(directedEdge);
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
        interLayerWindow(v);
    }

    /**
     * Add the given edge to timer queue to be evicted at updateTime
     */
    abstract public void intraLayerWindow(DirectedEdge directedEdge);

    /**
     * Add the given vertex to timer queue to be evicted at updateTime
     */
    abstract public void interLayerWindow(Vertex v);

    /**
     * Ae the timer maps ordered by timestamps
     */
    abstract public boolean hasOrderedTimestamps();

    /**
     * Evict waiting reduce messages less than the current timestamp
     */
    public final void evictReduceUntil(long timestamp) {
        try (BaseStorage.ObjectPoolScope objectPool = getRuntimeContext().getStorage().openObjectPoolScope()) {
            while (true) {
                // 1. Set placeholders
                final short currentPart = getPart();
                Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> maps = intraLayerMaps.get(currentPart);
                reuseVertexIdList.clear();
                reuseFeaturesNDList.clear();
                reuseIntList.clear();
                reuseIntList2.clear();
                reuseVertex2IndexMap.clear();

                // 2. Collect data
                ObjectBidirectionalIterator<Object2LongMap.Entry<String>> iterator = maps.f0.object2LongEntrySet().fastIterator();
                while (iterator.hasNext()) {
                    Object2LongMap.Entry<String> vertexTimerEntry = iterator.next();
                    if (reuseVertexIdList.size() >= MINI_BATCH_SIZE || (hasOrderedTimestamps() && vertexTimerEntry.getLongValue() > timestamp)) {
                        // Time not arrived yet
                        break;
                    }
                    reuseVertexIdList.add(vertexTimerEntry.getKey());
                    List<String> inEdges = maps.f1.remove(vertexTimerEntry.getKey());
                    for (String inVertexId : inEdges) {
                        getRuntimeContext().getStorage().addEdge(objectPool.getEdge(inVertexId, vertexTimerEntry.getKey(), null));
                        int indexOfSrc = reuseVertex2IndexMap.computeIfAbsent(inVertexId, (key) -> {
                            reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, key, "f").getValue());
                            return reuseFeaturesNDList.size() - 1;
                        });
                        reuseIntList.add(indexOfSrc);
                        objectPool.refresh();
                    }
                    reuseIntList2.add(reuseIntList.size());
                    iterator.remove();
                }

                // 3. Partial aggregate
                if (reuseVertexIdList.isEmpty()) return;
                NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
                reuseFeaturesNDList.clear();
                reuseFeaturesNDList.add(batchedFeatures);
                NDArray batchedSrcMessages = MESSAGE(reuseFeaturesNDList, false).get(0);
                // 4. Send
                int startIndex = 0;
                final int[] dim = new int[]{0};
                for (int i = 0; i < reuseIntList2.size(); i++) {
                    int[] indices = new int[reuseIntList2.getInt(i) - startIndex];
                    reuseFeaturesNDList.clear();
                    System.arraycopy(reuseIntList.elements(), startIndex, indices, 0, indices.length);
                    reuseFeaturesNDList.add(batchedSrcMessages.get(BaseNDManager.getManager().create(indices)).sum(dim));
                    reuseAggId.f1 = reuseVertexIdList.get(i);
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
                    startIndex += indices.length;
                }

                BaseNDManager.getManager().clean();
            }
        }
    }

    /**
     * Evict the waiting forward messages less than the current timestamp
     */
    public final void evictForwardUntil(long timestamp) {
        while (true) {
            // 1. Set placeholders
            final short currentPart = getPart();
            Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = interLayerMaps.get(getPart());
            reuseFeaturesNDList.clear();
            reuseAggregatorsNDList.clear();
            reuseVertexIdList.clear();

            // 2. Collect data
            try (BaseStorage.ObjectPoolScope objectPool = getRuntimeContext().getStorage().openObjectPoolScope()) {
                ObjectBidirectionalIterator<Object2LongMap.Entry<String>> iterator = maps.f0.object2LongEntrySet().fastIterator();
                while (iterator.hasNext()) {
                    Object2LongMap.Entry<String> vertexTimerEntry = iterator.next();
                    if (reuseVertexIdList.size() >= MINI_BATCH_SIZE || (hasOrderedTimestamps() && vertexTimerEntry.getLongValue() > timestamp)) {
                        // Time not arrived yet
                        break;
                    }
                    reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, vertexTimerEntry.getKey(), "f").getValue());
                    reuseAggregatorsNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, vertexTimerEntry.getKey(), "agg").getValue());
                    reuseVertexIdList.add(vertexTimerEntry.getKey());
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

            BaseNDManager.getManager().clean();
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
