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

    protected transient Map<Short, Object2LongOpenHashMap<String>> part2VertexTimers;

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
        part2VertexTimers = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("part2VertexTimers", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2VertexTimers.put(part, new Object2LongOpenHashMap<>()));
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
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 100.0) * 100);
        Object2LongOpenHashMap<String> vertexTimers = part2VertexTimers.get(getPart());
        vertexTimers.put(v.getId(), thisElementUpdateTime);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    /**
     * Evict the waiting forward messages less than the current timestamp
     */
    public void evictUpUntil(long timestamp) {
        short currentPart = getPart();
        Object2LongOpenHashMap<String> vertexTimers = part2VertexTimers.get(currentPart);
        reuseFeaturesNDList.clear();
        reuseAggregatorsNDList.clear();
        reuseVertexIdList.clear();
        try(BaseStorage.ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
            vertexTimers.forEach((key, val) -> {
                if (val <= timestamp) {
                    Vertex v = getRuntimeContext().getStorage().getVertex(key);
                    reuseFeaturesNDList.add((NDArray) (v.getFeature("f")).getValue());
                    reuseAggregatorsNDList.add((NDArray) (v.getFeature("agg")).getValue());
                    reuseVertexIdList.add(v.getId());
                    ignored.refresh();
                }
            });
        }
        if (reuseVertexIdList.isEmpty()) return;
        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        NDArray batchedAggregators = NDArrays.stack(reuseAggregatorsNDList);
        reuseFeaturesNDList.clear();
        reuseFeaturesNDList.add(batchedFeatures);reuseFeaturesNDList.add(batchedAggregators);
        NDArray batchedUpdates = UPDATE(reuseFeaturesNDList, false).get(0);
        for (int i = 0; i < reuseVertexIdList.size(); i++) {
            vertexTimers.removeLong(reuseVertexIdList.get(i));
            reuseTensor.value = batchedUpdates.get(i);
            reuseTensor.id.f1 = reuseVertexIdList.get(i);
            getRuntimeContext().output(new GraphOp(Op.COMMIT, currentPart, reuseTensor));
            throughput.inc();
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
