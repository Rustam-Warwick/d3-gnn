package plugins.gnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import elements.DirectedEdge;
import elements.Feature;
import elements.Plugin;
import elements.Vertex;
import elements.enums.ReplicaState;
import elements.features.InPlaceMeanAggregator;
import elements.features.InPlaceSumAggregator;
import elements.features.Tensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import plugins.ModelServer;

/**
 * Base class for all GNN Embedding Plugins
 */
abstract public class BaseGNNEmbedding extends Plugin {

    /**
     * Name of the {@link ai.djl.Model} name to fetch the {@link ModelServer}
     */
    public final String modelName;

    /**
     * Are vertex embeddings trainable or should it be expected from outside
     */
    public final boolean trainableVertexEmbeddings;

    /**
     * Fast reference to the {@link ModelServer} Plugin
     */
    public transient ModelServer<GNNBlock> modelServer;


    public BaseGNNEmbedding(String modelName, String suffix, boolean trainableVertexEmbeddings) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.trainableVertexEmbeddings = trainableVertexEmbeddings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        modelServer = (ModelServer<GNNBlock>) getRuntimeContext().getPlugin(String.format("%s-server", modelName));
    }

    /**
     * Calling the triggerUpdate function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature list
     * @param training training enabled
     * @return Next layer feature
     */
    public final NDList UPDATE(NDList feature, boolean training) {
        return (modelServer.getBlock()).update(modelServer.getParameterStore(), feature, training);
    }

    /**
     * Calling the output function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param features Source vertex Features or Batch
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public final NDList MESSAGE(NDList features, boolean training) {
        return (modelServer.getBlock()).message(modelServer.getParameterStore(), features, training);
    }

    /**
     * Is Edge ready for output passing
     *
     * @param directedEdge Edge
     * @return edge_ready
     */
    public final boolean messageReady(DirectedEdge directedEdge) {
        return directedEdge.getSrc().containsFeature("f");
    }

    /**
     * Are vertex features(embeddings) trainable
     *
     * @return are_trainable
     */
    public final boolean usingTrainableVertexEmbeddings() {
        return trainableVertexEmbeddings;
    }

    /**
     * Initialize the vertex aggregators and possible embeddings
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            Feature<?, ?> aggStart;
            switch (modelServer.getBlock().getAgg()) {
                case MEAN:
                    aggStart = new InPlaceMeanAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getOutputShapes()[0]), true);
                    break;
                case SUM:
                    aggStart = new InPlaceSumAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getOutputShapes()[0]), true);
                    break;
                default:
                    throw new IllegalStateException("Aggregator is not recognized");
            }
            aggStart.setElement(element, false);
            aggStart.createInternal();
            if (usingTrainableVertexEmbeddings() && getRuntimeContext().isFirst()) {
                Tensor embeddingRandom = new Tensor("f", BaseNDManager.getManager().ones(modelServer.getInputShapes()[0]), false); // Initialize to random value
                embeddingRandom.setElement(element, false);
                embeddingRandom.createInternal();
            }
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof TrainingSubCoordinator.StopStream) {
            listening = false;
        }else if(evt instanceof TrainingSubCoordinator.ResumeInference){
            listening = true;
        }
    }
}
