package plugins.gnn_embedding;

/**
 * Adaptive window based
 */
public class AdaptiveSessionWindowedGNNEmbedding extends StreamingGNNEmbedding {

    public AdaptiveSessionWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int sessionIntervalMs) {
        super(modelName, trainableVertexEmbeddings);
    }

}
