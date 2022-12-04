package plugins.gnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import elements.features.Tensor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionWindowedGNNEmbeddingLayer extends StreamingGNNEmbeddingLayer {

    public final int sessionInterval; // Window Interval for graph element updates in milliseconds

    public transient Map<Short, HashMap<String, Long>> BATCH; // Map for storing processingTimes

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    public SessionWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, int sessionInterval) {
        super(modelName, trainableVertexEmbeddings);
        this.sessionInterval = sessionInterval;
    }

    public SessionWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE, int sessionInterval) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
        this.sessionInterval = sessionInterval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        windowThroughput = new SimpleCounter();
        BATCH = new HashMap<>();
        getStorage().layerFunction.getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
    }

    public void forward(Vertex v) {
        long currentProcessingTime = getStorage().layerFunction.getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + sessionInterval;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 200.0) * 200);
        BATCH.computeIfAbsent(getStorage().layerFunction.getCurrentPart(), (ignored) -> new HashMap<>());
        HashMap<String, Long> PART_BATCH = BATCH.get(getStorage().layerFunction.getCurrentPart());
        PART_BATCH.put(v.getId(), thisElementUpdateTime);
        getStorage().layerFunction.getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    /**
     * Actually send the elements
     *
     * @param timestamp firing timestamp
     */
    @Override
    public void onTimer(long timestamp) {
        super.onTimer(timestamp);
        try {
            BaseNDManager.getManager().delay();
            HashMap<String, Long> PART_BATCH = BATCH.get(getStorage().layerFunction.getCurrentPart());
            NDList features = new NDList();
            NDList aggregators = new NDList();
            List<Vertex> vertices = new ArrayList<>();
            PART_BATCH.forEach((key, val) -> {
                if (val <= timestamp) {
                    Vertex v = getStorage().getVertex(key);
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
                updateTensor.ids.f0 = ElementType.VERTEX;
                updateTensor.ids.f1 = messageVertex.getId();
                getStorage().layerFunction.message(new GraphOp(Op.COMMIT, updateTensor.getMasterPart(), updateTensor), MessageDirection.FORWARD);
                throughput.inc();
            }
        } catch (Exception e) {
            LOG.error(ExceptionUtils.stringifyException(e));
        } finally {
            BaseNDManager.getManager().resume();
        }
    }

}
