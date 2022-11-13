package plugins.hgnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.HGNNBlock;
import elements.HEdge;
import elements.Plugin;
import elements.Vertex;
import elements.enums.ReplicaState;
import features.InPlaceMeanAggregator;
import features.MeanAggregator;
import features.Tensor;
import plugins.ModelServer;

/**
 * Base class for all HGNN Embedding Plugins
 * message is ready when source f is ready
 * update is ready when source f and agg are ready
 */
abstract public class BaseHGNNEmbeddingPlugin extends Plugin {

    public final String modelName;

    public final boolean trainableVertexEmbeddings;

    public boolean IS_ACTIVE;

    public transient ModelServer<HGNNBlock> modelServer;

    public BaseHGNNEmbeddingPlugin(String modelName, String suffix) {
        this(modelName, suffix, false);
    }

    public BaseHGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings) {
        this(modelName, suffix, trainableVertexEmbeddings, true);
    }

    public BaseHGNNEmbeddingPlugin(String modelName, String suffix, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.trainableVertexEmbeddings = trainableVertexEmbeddings;
        this.IS_ACTIVE = IS_ACTIVE;
    }

    @Override
    public void open() throws Exception {
        super.open();
        modelServer = (ModelServer<HGNNBlock>) storage.getPlugin(String.format("%s-server", modelName));
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
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
     * Is Vertex ready to send message to aggregator
     */
    public final boolean messageReady(Vertex v) {
        return v.containsFeature("f");
    }

    /**
     * Is vertex ready for update
     *
     * @param vertex Vertex
     * @return vertex_ready
     */
    public final boolean updateReady(Vertex vertex) {
        return vertex.state() == ReplicaState.MASTER && vertex.containsFeature("f");
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

    /**
     * Initialize the vertex aggregators and possible embeddings
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = storage.layerFunction.getWrapperContext().getNDManager().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new InPlaceMeanAggregator(aggStart, true, (short) -1));
            if (usingTrainableVertexEmbeddings() && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = storage.layerFunction.getWrapperContext().getNDManager().randomNormal(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("f", new Tensor(embeddingRandom));
            }
        }
    }

    /**
     * Initialize the hyper-edge aggregators
     */
    public void initHyperEdge(HEdge edge) {
        if (edge.state() == ReplicaState.MASTER) {
            NDArray aggStart = storage.layerFunction.getWrapperContext().getNDManager().zeros(modelServer.getInputShape().get(0).getValue());
            edge.setFeature("agg", new MeanAggregator(aggStart, false, (short) -1));
        }
    }
}
