package hgnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.nn.hgnn.HGNNBlock;
import elements.Feature;
import elements.HyperEdge;
import elements.Plugin;
import elements.Vertex;
import elements.enums.ReplicaState;
import elements.features.*;
import org.apache.flink.configuration.Configuration;
import plugins.ModelServer;

/**
 * Base class for all HGNN Embedding Plugins
 * output is ready when source f is ready
 * triggerUpdate is ready when source f and agg are ready
 */
abstract public class BaseHGNNEmbeddingPlugin extends Plugin {

    public final String modelName;

    public final boolean trainableVertexEmbeddings;

    public transient ModelServer<HGNNBlock> modelServer;

    public BaseHGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.trainableVertexEmbeddings = trainableVertexEmbeddings;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        modelServer = (ModelServer<HGNNBlock>) getRuntimeContext().getPlugin(String.format("%s-server", modelName));
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
     * Is Vertex ready to send output to aggregator
     */
    public final boolean messageReady(Vertex v) {
        return v.containsFeature("f");
    }

    /**
     * Are vertex features(embeddings) trainable
     *
     * @return are_trainable
     */
    public boolean usingTrainableVertexEmbeddings() {
        return trainableVertexEmbeddings;
    }

    /**
     * Is the output batched or streaming
     *
     * @return is_batched
     */
    public boolean usingBatchingOutput() {
        return false;
    }

    /**
     * Initialize Vertex Aggregator and possible embeeddings
     */
    public void initVertex(Vertex vertex) {
        if (vertex.state() == ReplicaState.MASTER) {
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
            aggStart.setElement(vertex, false);
            aggStart.createInternal();
            if (usingTrainableVertexEmbeddings() && getRuntimeContext().isFirst()) {
                Tensor embeddingRandom = new Tensor("f", BaseNDManager.getManager().ones(modelServer.getInputShapes()[0])); // Initialize to random value
                embeddingRandom.setElement(vertex, false);
                embeddingRandom.createInternal();
            }
        }
    }

    /**
     * Initialize the hyper-edge aggregators
     */
    public void initHyperEdge(HyperEdge hyperEdge) {
        if (hyperEdge.state() == ReplicaState.MASTER) {
            Feature<?, ?> aggStart;
            switch (modelServer.getBlock().getHyperEdgeAgg()) {
                case MEAN:
                    aggStart = new MeanAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getOutputShapes()[0]));
                    break;
                case SUM:
                    aggStart = new SumAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getOutputShapes()[0]));
                    break;
                default:
                    throw new IllegalStateException("Aggregator is not recognized");
            }
            aggStart.setElement(hyperEdge, false);
            aggStart.createInternal();
        }
    }
}
