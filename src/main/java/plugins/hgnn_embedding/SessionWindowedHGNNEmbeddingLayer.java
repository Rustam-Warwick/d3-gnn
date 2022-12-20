package plugins.hgnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.features.Tensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionWindowedHGNNEmbeddingLayer extends StreamingHGNNEmbeddingLayer {

    public final int sessionInterval; // Window Interval for graph element updates in milliseconds

    public transient Map<Short, HashMap<String, Long>> BATCH; // Map for storing processingTimes

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    public SessionWindowedHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, int sessionInterval) {
        super(modelName, trainableVertexEmbeddings);
        this.sessionInterval = sessionInterval;
    }

    public SessionWindowedHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE, int sessionInterval) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
        this.sessionInterval = sessionInterval;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        windowThroughput = new SimpleCounter();
        BATCH = new HashMap<>();
        getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
    }

    @Override
    public void forward(Vertex v) {
        long currentProcessingTime = getRuntimeContext().getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + sessionInterval;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 200.0) * 200);
        BATCH.computeIfAbsent(getRuntimeContext().getCurrentPart(), (ignored) -> new HashMap<>());
        HashMap<String, Long> PART_BATCH = BATCH.get(getRuntimeContext().getCurrentPart());
        PART_BATCH.put(v.getId(), thisElementUpdateTime);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    @Override
    public void onProcessingTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {
        super.onProcessingTime(timer);
        try {
            BaseNDManager.getManager().delay();
            HashMap<String, Long> PART_BATCH = BATCH.get(getRuntimeContext().getCurrentPart());
            NDList features = new NDList();
            NDList aggregators = new NDList();
            List<Vertex> vertices = new ArrayList<>();
            PART_BATCH.forEach((key, val) -> {
                if (val <= timer.getTimestamp()) {
                    Vertex v = getRuntimeContext().getStorage().getVertex(key);
                    features.add((NDArray) (v.getFeature("f")).getValue());
                    aggregators.add((NDArray) (v.getFeature("agg")).getValue());
                    vertices.add(v);
                }
            });
            if (vertices.isEmpty()) return;
            NDList batchedInput = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
            NDArray batchedUpdates = UPDATE(batchedInput, false).get(0);
            for (int i = 0; i < vertices.size(); i++) {
                PART_BATCH.remove(vertices.get(i).getId());
                Vertex messageVertex = vertices.get(i);
                Tensor updateTensor = new Tensor("f", batchedUpdates.get(i), false, messageVertex.getMasterPart());
                updateTensor.id.f0 = ElementType.VERTEX;
                updateTensor.id.f1 = messageVertex.getId();
                getRuntimeContext().output(new GraphOp(Op.COMMIT, messageVertex.getMasterPart(), updateTensor));
                throughput.inc();
            }
        } catch (Exception e) {
            LOG.error(ExceptionUtils.stringifyException(e));
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
