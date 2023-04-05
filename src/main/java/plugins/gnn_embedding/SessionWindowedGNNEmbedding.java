package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.taskshared.TaskSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import storage.BaseStorage;

import java.util.Map;

/**
 * GNN Embedding Layer that forwards messages only on pre-defined sessioned intervals
 */
public class SessionWindowedGNNEmbedding extends StreamingGNNEmbedding {

    public final int sessionIntervalMs;

    protected transient Map<Short, Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>>> part2VertexMaps;

    protected transient Counter windowThroughput;

    protected transient NDList reuseAggregatorsNDList;

    protected transient ObjectArrayList<String> reuseVertexIdList;

    public SessionWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int sessionIntervalMs) {
        super(modelName, trainableVertexEmbeddings);
        this.sessionIntervalMs = sessionIntervalMs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseAggregatorsNDList = new NDList();
        reuseVertexIdList = new ObjectArrayList<>();
        part2VertexMaps = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("window_part2VertexMaps", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2VertexMaps.put(part, Tuple2.of(new Object2LongLinkedOpenHashMap<>(), new Object2LongOpenHashMap<>())));
        windowThroughput = new SimpleCounter();
        getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
    }

    /**
     * {@inheritDoc}
     * Adds the forward messages to timer queue and does not immediately forward it to the next layer
     */
    public void forward(Vertex v) {
        long currentProcessingTime = getRuntimeContext().getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + sessionIntervalMs;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 25.0) * 25);
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = part2VertexMaps.get(getPart());
        maps.f0.removeLong(v.getId());
        maps.f0.put(v.getId(), thisElementUpdateTime);
        maps.f1.put(v.getId(), getRuntimeContext().currentTimestamp());
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    /**
     * Evict the waiting forward messages less than the current timestamp
     */
    public void evictUpUntil(long timestamp) {

        // 1. Set placeholders
        final short currentPart = getPart();
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = part2VertexMaps.get(getPart());
        reuseFeaturesNDList.clear();
        reuseAggregatorsNDList.clear();
        reuseVertexIdList.clear();

        // 2. Collect data
        try (BaseStorage.ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
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
                ignored.refresh();
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
        evictUpUntil(timer.getTimestamp());
    }

}
