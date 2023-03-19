package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.features.Tensor;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GNN Embedding Layer that forwards messages only on pre-defined sessioned intervals
 */
public class SessionWindowedGNNEmbedding extends StreamingGNNEmbedding {

    public final int sessionIntervalMs;

    protected transient Map<Short, HashMap<String, Long>> BATCH;

    protected transient Counter windowThroughput;

    protected transient NDList reuseFeaturesNDList;

    protected transient NDList reuseAggregatorsNDList;

    protected transient List<String> reuseVertexIdList;

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
        reuseFeaturesNDList = new NDList();
        reuseVertexIdList = new ObjectArrayList<>();
        BATCH = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("BATCH", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> BATCH.put(part, new HashMap<>()));
        windowThroughput = new SimpleCounter();
        getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
    }

    /**
     * {@inheritDoc}
     * Adds the forward messages to timer queue
     */
    public void forward(Vertex v) {
        long currentProcessingTime = getRuntimeContext().getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + sessionIntervalMs;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 100.0) * 100);
        HashMap<String, Long> PART_BATCH = BATCH.get(getPart());
        PART_BATCH.put(v.getId(), thisElementUpdateTime);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    /**
     * Evict the waiting forward messages less than the current timestamp
     */
    public void evictUpUntil(long timestamp) {
        short currentPart = getPart();
        HashMap<String, Long> PART_BATCH = BATCH.get(currentPart);
        reuseFeaturesNDList.clear();
        reuseAggregatorsNDList.clear();
        reuseNDList.clear();
        reuseVertexIdList.clear();
        PART_BATCH.forEach((key, val) -> {
            if (val <= timestamp) {
                try(BaseStorage.ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
                    Vertex v = getRuntimeContext().getStorage().getVertex(key);
                    reuseFeaturesNDList.add((NDArray) (v.getFeature("f")).getValue());
                    reuseAggregatorsNDList.add((NDArray) (v.getFeature("agg")).getValue());
                    reuseVertexIdList.add(v.getId());
                }
            }
        });
        if (reuseVertexIdList.isEmpty()) return;
        reuseNDList.add(NDArrays.stack(reuseFeaturesNDList));
        reuseNDList.add(NDArrays.stack(reuseAggregatorsNDList));
        NDArray batchedUpdates = UPDATE(reuseNDList, false).get(0);
        Tensor tmpTensor = new Tensor("f", null, false);
        for (int i = 0; i < reuseVertexIdList.size(); i++) {
            PART_BATCH.remove(reuseVertexIdList.get(i));
            tmpTensor.value = batchedUpdates.get(i);
            tmpTensor.id.f0 = ElementType.VERTEX;
            tmpTensor.id.f1 = reuseVertexIdList.get(i);
            getRuntimeContext().output(new GraphOp(Op.COMMIT, currentPart, tmpTensor));
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
