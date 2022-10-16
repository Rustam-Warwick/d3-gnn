package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.*;
import features.Tensor;
import plugins.ModelServer;

/**
 * Base class for all GNN Embedding Plugins
 */
abstract public class BaseGNNEmbeddingPlugin extends Plugin {

    public final String modelName;

    public final boolean trainableVertexEmbeddings;

    public boolean IS_ACTIVE;

    public transient ModelServer modelServer;

    public BaseGNNEmbeddingPlugin(String modelName, String suffix) {
        this(modelName, suffix, false);
    }

    public BaseGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings) {
        this(modelName, suffix, trainableVertexEmbeddings, true);
    }

    public BaseGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.trainableVertexEmbeddings = trainableVertexEmbeddings;
        this.IS_ACTIVE = IS_ACTIVE;
    }

    @Override
    public void open() throws Exception {
        super.open();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature list
     * @param training training enabled
     * @return Next layer feature
     */
    public NDList UPDATE(NDList feature, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getUpdateBlock().forward(modelServer.getParameterStore(), feature, training);
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param features Source vertex Features or Batch
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public NDList MESSAGE(NDList features, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), features, training);

    }

    /**
     * Is Edge ready for message passing
     *
     * @param edge Edge
     * @return edge_ready
     */
    public boolean messageReady(Edge edge) {
        return edge.getSrc().containsFeature("f");
    }

    /**
     * Is vertex ready for update
     *
     * @param vertex Vertex
     * @return vertex_ready
     */
    public boolean updateReady(Vertex vertex) {
        return vertex != null && vertex.state() == ReplicaState.MASTER && vertex.containsFeature("f") && vertex.containsFeature("agg");
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

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (IS_ACTIVE && element.elementType() == ElementType.VERTEX && element.state() == ReplicaState.MASTER) {
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(aggStart, true));
            if (usingTrainableVertexEmbeddings() && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = LifeCycleNDManager.getInstance().randomNormal(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("f", new Tensor(embeddingRandom));
            }
        }
    }
}
