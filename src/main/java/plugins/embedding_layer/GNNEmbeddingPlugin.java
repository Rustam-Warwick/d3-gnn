package plugins.embedding_layer;

import ai.djl.ndarray.NDList;
import elements.Edge;
import elements.Vertex;

public interface GNNEmbeddingPlugin {
    boolean updateReady(Vertex v);

    boolean messageReady(Edge e);

    NDList MESSAGE(NDList inputs, boolean training);

    NDList UPDATE(NDList inputs, boolean training);

    boolean usingTrainableEmbeddings();

    boolean usingBatchingOutput();

}
