package plugins.gnn_embedding;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import elements.DirectedEdge;
import elements.Plugin;
import elements.Vertex;
import elements.enums.ReplicaState;
import features.InPlaceMeanAggregator;
import features.Tensor;
import plugins.ModelServer;

/**
 * Base class for all GNN Embedding Plugins
 */
abstract public class BaseGNNEmbeddingPlugin extends Plugin {

    public final String modelName;

    public final boolean trainableVertexEmbeddings;

    public final boolean requiresDestForMessage;

    public boolean IS_ACTIVE;

    public transient ModelServer<GNNBlock> modelServer;

    public BaseGNNEmbeddingPlugin(String modelName, String suffix) {
        this(modelName, suffix, false);
    }

    public BaseGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings) {
        this(modelName, suffix, trainableVertexEmbeddings, true);
    }

    public BaseGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        this(modelName, suffix, trainableVertexEmbeddings, false, IS_ACTIVE);
    }

    public BaseGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings, boolean requiresDestForMessage, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.trainableVertexEmbeddings = trainableVertexEmbeddings;
        this.IS_ACTIVE = IS_ACTIVE;
        this.requiresDestForMessage = requiresDestForMessage;
    }

    @Override
    public void open() throws Exception {
        super.open();
        modelServer = (ModelServer<GNNBlock>) getStorage().getPlugin(String.format("%s-server", modelName));
    }

    /**
     * Calling the triggerUpdate function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature list
     * @param training training enabled
     * @return Next layer feature
     */
    public final NDList UPDATE(NDList feature, boolean training) {
        return (modelServer.getBlock()).getUpdateBlock().forward(modelServer.getParameterStore(), feature, training);
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param features Source vertex Features or Batch
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public final NDList MESSAGE(NDList features, boolean training) {
        return (modelServer.getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), features, training);

    }

    /**
     * Is Edge ready for message passing
     *
     * @param directedEdge Edge
     * @return edge_ready
     */
    public final boolean messageReady(DirectedEdge directedEdge) {
        return requiresDestForMessage ? directedEdge.getSrc().containsFeature("f") && directedEdge.getDest().containsFeature("f") : directedEdge.getSrc().containsFeature("f");
    }

    /**
     * Is vertex ready for triggerUpdate
     *
     * @param vertex Vertex
     * @return vertex_ready
     */
    public final boolean updateReady(Vertex vertex) {
        return vertex.state() == ReplicaState.MASTER && vertex.containsFeature("f") & vertex.containsFeature("agg");
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
     * Is the output batched or streaming
     *
     * @return is_batched
     */
    public boolean usingBatchingOutput() {
        return false;
    }

    /**
     * Stop this plugin
     */
    public void stop() {
        IS_ACTIVE = false;
    }

    /**
     * Start this plugin
     */
    public void start() {
        IS_ACTIVE = true;
    }

    /**
     * Initialize the vertex aggregators and possible embeddings
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            InPlaceMeanAggregator aggStart = new InPlaceMeanAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getInputShape().get(0).getValue()), true);
            aggStart.setElement(element, false);
            getStorage().runCallback(aggStart.createInternal());
            if (usingTrainableVertexEmbeddings() && getStorage().layerFunction.isFirst()) {
                Tensor embeddingRandom = new Tensor("f", BaseNDManager.getManager().randomNormal(modelServer.getInputShape().get(0).getValue()), false); // Initialize to random value
                embeddingRandom.setElement(element, false);
                getStorage().runCallback(embeddingRandom.createInternal());
            }
        }
    }
}
