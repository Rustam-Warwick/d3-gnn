package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import features.Tensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CountWindowedGNNEmbeddingLayer extends StreamingGNNEmbeddingLayer {

    public final int BATCH_SIZE; // Batch size of the operator as a whole

    public transient int LOCAL_BATCH_SIZE; // Batch size for distributed job depending on maxParallelism

    public transient Batchifier batchifier; // Batchifier for the windowed data

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    public CountWindowedGNNEmbeddingLayer(String modelName, int BATCH_SIZE) {
        super(modelName);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    public CountWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, int BATCH_SIZE) {
        super(modelName, trainableVertexEmbeddings);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    public CountWindowedGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE, int BATCH_SIZE) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // assert storage != null;
        LOCAL_BATCH_SIZE = BATCH_SIZE / storage.layerFunction.getRuntimeContext().getMaxNumberOfParallelSubtasks();
        batchifier = new StackBatchifier();
        windowThroughput = new SimpleCounter();
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
        storage.layerFunction.getWrapperContext().runForAllKeys(() -> {
            Feature<Tuple2<Integer, Set<String>>, Tuple2<Integer, Set<String>>> elementUpdates = new Feature<>("elementUpdates", Tuple2.of(0, new HashSet<>()), true, storage.layerFunction.getCurrentPart());
            elementUpdates.setStorage(storage);
            elementUpdates.create();
        });
    }

    public void forward(Vertex v) {
        Feature<Tuple2<Integer, Set<String>>, Tuple2<Integer, Set<String>>> elementUpdates = (Feature<Tuple2<Integer, Set<String>>, Tuple2<Integer, Set<String>>>) storage.getStandaloneFeature("elementUpdates");
        elementUpdates.getValue().f1.add(v.getId());
        if (elementUpdates.getValue().f0++ >= LOCAL_BATCH_SIZE) {
            List<NDList> inputs = new ArrayList<>();
            List<Vertex> vertices = new ArrayList<>();
            elementUpdates.getValue().f1.forEach((key) -> {
                Vertex tmpV = storage.getVertex(key);
                if (updateReady(tmpV)) {
                    NDArray ft = (NDArray) (tmpV.getFeature("f")).getValue();
                    NDArray agg = (NDArray) (tmpV.getFeature("agg")).getValue();
                    inputs.add(new NDList(ft, agg));
                    vertices.add(tmpV);
                }
            });
            if (inputs.isEmpty()) return;
            NDList batch_inputs = batchifier.batchify(inputs.toArray(NDList[]::new));
            NDList batch_updates = UPDATE(batch_inputs, false);
            NDList[] updates = batchifier.unbatchify(batch_updates);
            Tensor updateTensor = new Tensor("f", null, false, getPartId());
            for (int i = 0; i < updates.length; i++) {
                throughput.inc();
                Vertex messageVertex = vertices.get(i);
                updateTensor.attachedTo = Tuple3.of(ElementType.VERTEX, messageVertex.getId(), null);
                updateTensor.value = updates[i].get(0);
                storage.layerFunction.message(new GraphOp(Op.COMMIT, updateTensor.masterPart(), updateTensor), MessageDirection.FORWARD);
            }
            elementUpdates.getValue().f1.clear();
            elementUpdates.getValue().f0 = 0;
        }

    }

}
