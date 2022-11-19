package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import features.Tensor;
import operators.BaseWrapperOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TimeWindowedGNNEmbeddingLayer extends StreamingGNNEmbeddingLayer {

    public final int windowInterval; // Window Interval for graph element updates in milliseconds

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    public TimeWindowedGNNEmbeddingLayer(String modelName, int windowInterval) {
        super(modelName);
        this.windowInterval = windowInterval;
    }

    public TimeWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, int windowInterval) {
        super(modelName, trainableVertexEmbeddings);
        this.windowInterval = windowInterval;
    }

    public TimeWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE, int windowInterval) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
        this.windowInterval = windowInterval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        windowThroughput = new SimpleCounter();
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
        try {
            storage.layerFunction.getWrapperContext().runForAllKeys(() -> {
                Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = new Feature<>("elementUpdates", new HashMap<>(), true, storage.layerFunction.getCurrentPart());
                elementUpdates.setStorage(storage);
                elementUpdates.create();
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void forward(Vertex v) {
        long currentProcessingTime = storage.layerFunction.getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + windowInterval;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 100.0) * 100);
        Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = (Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>>) storage.getStandaloneFeature("elementUpdates");
        elementUpdates.getValue().put(v.getId(), Tuple2.of(thisElementUpdateTime, storage.layerFunction.currentTimestamp()));
        storage.updateElement(elementUpdates, elementUpdates);
        storage.layerFunction.getTimerService().registerProcessingTimeTimer(timerTime);
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
            storage.layerFunction.getWrapperContext().getNDManager().delay();
            Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = (Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>>) storage.getStandaloneFeature("elementUpdates");
            NDList features = new NDList();
            NDList aggregators = new NDList();
            List<Vertex> vertices = new ArrayList<>();
            elementUpdates.getValue().forEach((key, val) -> {
                if (val.f0 <= timestamp) {
                    Vertex v = storage.getVertex(key);
                    features.add((NDArray) (v.getFeature("f")).getValue());
                    aggregators.add((NDArray) (v.getFeature("agg")).getValue());
                    vertices.add(v);
                }
            });
            if (vertices.isEmpty()) return;
            NDList batchedInput = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
            NDArray batchedUpdates = UPDATE(batchedInput, false).get(0);
            for (int i = 0; i < vertices.size(); i++) {
                elementUpdates.getValue().remove(vertices.get(i).getId());
                Vertex messageVertex = vertices.get(i);
                Tensor updateTensor = new Tensor("f", batchedUpdates.get(i), false, messageVertex.masterPart());
                updateTensor.attachedTo.f0 = ElementType.VERTEX;
                updateTensor.attachedTo.f1 = messageVertex.getId();
                storage.layerFunction.message(new GraphOp(Op.COMMIT, updateTensor.masterPart(), updateTensor), MessageDirection.FORWARD);
                throughput.inc();
            }
        } catch (Exception e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
        } finally {
            storage.layerFunction.getWrapperContext().getNDManager().resume();
        }
    }

}
