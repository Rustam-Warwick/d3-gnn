package plugins.gnn_embedding;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import elements.Vertex;
import org.apache.flink.configuration.Configuration;

/**
 * Adaptive windowing based on exponential-mean of the vertex forward messages
 */
public class DeepAdaptiveWindowedGNNEmbeddingDeep extends DeepSessionWindowedGNNEmbedding {

    protected transient CountMinSketch forwardHitCounts;

    public DeepAdaptiveWindowedGNNEmbeddingDeep(String modelName, boolean trainableVertexEmbeddings, int sessionIntervalMs) {
        super(modelName, trainableVertexEmbeddings, sessionIntervalMs);
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        forwardHitCounts = new CountMinSketch(0.001, 0.99, 1);
    }

    @Override
    public void forward(Vertex v) {
        forwardHitCounts.add(v.getId(), 1);
        super.forward(v);
    }

}
