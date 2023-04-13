package plugins.gnn_embedding;

import elements.DirectedEdge;
import elements.Vertex;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.tmshared.TMSharedExpMovingAverageCountMinSketch;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;

/**
 * Adaptive windowing based on exponential-mean of the vertex forward messages
 */
public class AdaptiveWindowedGNNEmbedding extends WindowedGNNEmbedding {

    protected final long movingAverageIntervalMs;
    protected final double momentum;
    protected transient TMSharedExpMovingAverageCountMinSketch forwardExpMean;

    public AdaptiveWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, long movingAverageIntervalMs, double momentum) {
        super(modelName, trainableVertexEmbeddings);
        this.movingAverageIntervalMs = movingAverageIntervalMs;
        this.momentum = momentum;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        forwardExpMean = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_forwardExpMean", TypeInformation.of(TMSharedExpMovingAverageCountMinSketch.class), () -> new TMSharedExpMovingAverageCountMinSketch(0.001, 0.99, movingAverageIntervalMs, momentum)));
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {

    }

    @Override
    public void interLayerWindow(Vertex v) {

    }

}
