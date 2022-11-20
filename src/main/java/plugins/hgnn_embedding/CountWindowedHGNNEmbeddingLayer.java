package plugins.hgnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import features.Tensor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class CountWindowedHGNNEmbeddingLayer extends StreamingHGNNEmbeddingLayer {

    public final int BATCH_SIZE; // Batch size of the operator as a whole

    public transient int LOCAL_BATCH_SIZE; // Batch size for distributed job depending on maxParallelism

    public transient Map<Short, Tuple2<Integer, Set<String>>> BATCH;

    public CountWindowedHGNNEmbeddingLayer(String modelName, int BATCH_SIZE) {
        super(modelName);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    public CountWindowedHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, int BATCH_SIZE) {
        super(modelName, trainableVertexEmbeddings);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    public CountWindowedHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE, int BATCH_SIZE) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
        this.BATCH_SIZE = BATCH_SIZE;
    }

    @Override
    public void open() throws Exception {
        super.open();
        LOCAL_BATCH_SIZE = BATCH_SIZE / storage.layerFunction.getRuntimeContext().getMaxNumberOfParallelSubtasks();
        BATCH = new HashMap<>();
    }

    public void forward(Vertex v) {
        BATCH.computeIfAbsent(storage.layerFunction.getCurrentPart(), (ignored) -> Tuple2.of(0, new HashSet<>()));
        Tuple2<Integer, Set<String>> PART_BATCH = BATCH.get(storage.layerFunction.getCurrentPart());
        PART_BATCH.f1.add(v.getId());
        if (++PART_BATCH.f0 > LOCAL_BATCH_SIZE) {
            List<Vertex> vertices = new ArrayList<>();
            NDList features = new NDList();
            NDList aggregators = new NDList();
            PART_BATCH.f1.forEach((key) -> {
                Vertex vTmp = storage.getVertex(key);
                features.add((NDArray) (vTmp.getFeature("f")).getValue());
                aggregators.add((NDArray) (vTmp.getFeature("agg")).getValue());
                vertices.add(vTmp);
            });
            NDList batchedInput = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
            NDArray batchedUpdates = UPDATE(batchedInput, false).get(0);
            for (int i = 0; i < vertices.size(); i++) {
                Vertex messageVertex = vertices.get(i);
                Tensor updateTensor = new Tensor("f", batchedUpdates.get(i), false, messageVertex.masterPart());
                updateTensor.attachedTo.f0 = ElementType.VERTEX;
                updateTensor.attachedTo.f1 = messageVertex.getId();
                storage.layerFunction.message(new GraphOp(Op.COMMIT, updateTensor.masterPart(), updateTensor), MessageDirection.FORWARD);
                throughput.inc();
            }
            PART_BATCH.f0 = 0;
            PART_BATCH.f1.clear();
        }
    }

}
