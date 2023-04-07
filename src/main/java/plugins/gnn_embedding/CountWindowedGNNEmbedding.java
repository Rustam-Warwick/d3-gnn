package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.tmshared.states.TMSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import storage.ObjectPoolScope;

import java.util.Map;

/**
 * GNN Embedding layer based on a certain buffer count
 */
public class CountWindowedGNNEmbedding extends StreamingGNNEmbedding {

    public final int BATCH_SIZE;

    public transient int LOCAL_BATCH_SIZE;

    public transient Map<Short, Tuple2<Integer, Object2LongOpenHashMap<String>>> part2VertexMaps;

    protected transient NDList reuseAggregatorsNDList;

    protected transient ObjectArrayList<String> reuseVertexIdList;

    public CountWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int BATCH_SIZE) {
        super(modelName, trainableVertexEmbeddings);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseAggregatorsNDList = new NDList();
        reuseVertexIdList = new ObjectArrayList<>();
        LOCAL_BATCH_SIZE = BATCH_SIZE / getRuntimeContext().getNumberOfParallelSubtasks();
        part2VertexMaps = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_part2VertexMaps", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2VertexMaps.put(part, Tuple2.of(0, new Object2LongOpenHashMap<>())));
    }

    public void forward(Vertex v) {
        Tuple2<Integer, Object2LongOpenHashMap<String>> maps = part2VertexMaps.get(getPart());
        maps.f1.put(v.getId(), getRuntimeContext().currentTimestamp());
        if (++maps.f0 >= LOCAL_BATCH_SIZE) {
            batchedForward();
            maps.f0 = 0;
        }
    }

    public void batchedForward() {

        // 1. Set placeholders
        final short currentPart = getPart();
        Tuple2<Integer, Object2LongOpenHashMap<String>> maps = part2VertexMaps.get(currentPart);
        reuseFeaturesNDList.clear();
        reuseAggregatorsNDList.clear();
        reuseVertexIdList.clear();

        // 2. Collect data
        try (ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
            maps.f1.forEach((key, ts) -> {
                Vertex v = getRuntimeContext().getStorage().getVertices().get(key);
                reuseFeaturesNDList.add((NDArray) (v.getFeature("f")).getValue());
                reuseAggregatorsNDList.add((NDArray) (v.getFeature("agg")).getValue());
                reuseVertexIdList.add(v.getId());
                ignored.refresh();
            });
        }

        // 3. Forward
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

}
