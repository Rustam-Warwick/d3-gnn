package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.gnn.GNNBlock;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import elements.iterations.Rmi;
import features.Tensor;
import functions.nn.MyParameterStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * For each Edge, Vertex, Feature addition preforms 1 layer of GNN Embedding on a periodical fashion
 * Outputs -> New Feature to the next layer
 *
 * @implNote Expects Edges to have timestamps
 */
public class GNNPeriodicalEmbeddingLayer extends Plugin {
    // ---------------------- MODEL ---------------------

    public Model model; // Model with block of GNNLayer
    public transient Shape inputShape; // InputShape for this model
    public transient MyParameterStore parameterStore;


    // ---------------------- RUNTIME RELATED -------------------
    public boolean externalFeatures; // Do we expect external features or have to initialize features on the first layer
    public boolean ACTIVE = true; // Is the plugin currently running
    public long last_reduce = Long.MIN_VALUE; // Last reduce watermark all edges <= this value should be reduced
    public int acknowledgedReduceCount = 0; // Count of broadcaster reduce messages
    public long last_update = Long.MIN_VALUE; // Last update watermark. All features <= this value should be updated


    public GNNPeriodicalEmbeddingLayer(Model model, boolean externalFeatures) {
        super("inferencer");
        this.externalFeatures = externalFeatures;
        this.model = model;
    }

    @Override
    public void open() {
        super.open();
        parameterStore = new MyParameterStore(BaseNDManager.threadNDManager.get());
        parameterStore.loadModel(model);
        inputShape = model.describeInput().get(0).getValue();
    }

    @Override
    public void close() {
        super.close();
        model.close();
    }

    @Override
    @SuppressWarnings("all")
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName()) && ACTIVE) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
            }
        }
    }

    @Override
    public void onWatermark(long timestamp) {
        super.onWatermark(timestamp);
        for (Vertex v : storage.getVertices()) {
            reduceInEdges(v, timestamp);
        }
        if (replicaParts().isEmpty() || getPartId() == replicaParts().get(replicaParts().size() - 1)) {
            // This is the last part of this operator
            last_reduce = timestamp;
            Rmi broadCastMessage = new Rmi(getId(), "acknowledgeReduce", new Object[0], ElementType.PLUGIN, false, timestamp);
            storage.layerFunction.broadcastMessage(new GraphOp(Op.RMI, getPartId(), broadCastMessage, timestamp), MessageDirection.ITERATE);
        }
    }

    @RemoteFunction
    public void acknowledgeReduce() {
        if (state() == ReplicaState.MASTER) {
            ++acknowledgedReduceCount;
        }
        if (acknowledgedReduceCount == storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks()) {
            for (Vertex v : storage.getVertices()) {
                if (updateReady(v)) {
                    long nextFeatureTimestamp = Math.max(v.getFeature("feature").getTimestamp(), v.getFeature("agg").getTimestamp());
                    if (nextFeatureTimestamp > last_update) {
                        forward(v);
                    }
                }
            }

            if (replicaParts().isEmpty() || getPartId() == replicaParts().get(replicaParts().size() - 1)) {
                acknowledgedReduceCount = 0;
                last_update = last_reduce;
            }
        }
    }

    /**
     * Reduce all the in-edges that came in last_WT<t<=WT
     *
     * @param v         vertex to reduce into
     * @param timestamp max timestamp of edges to include
     */
    public void reduceInEdges(Vertex v, long timestamp) {
        Iterable<Edge> inEdges = storage.getIncidentEdges(v, EdgeType.IN);
        List<NDArray> ndArrays = new ArrayList<>();
        long maxTimestamp = Long.MIN_VALUE;
        for (Edge e : inEdges) {
            if (e.getTimestamp() > last_reduce && e.getTimestamp() <= timestamp && messageReady(e)) {
                NDArray tmp = message((NDArray) e.src.getFeature("feature").getValue(), false);
                ndArrays.add(tmp);
                maxTimestamp = Math.max(Math.max(e.getTimestamp(), e.src.getFeature("feature").getTimestamp()), maxTimestamp);
            }
        }
        if (ndArrays.size() > 0) {
            NDArray msg = MeanAggregator.bulkReduce(ndArrays.toArray(NDArray[]::new));
            new RemoteInvoke()
                    .toElement(v.decodeFeatureId("agg"), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(v.masterPart())
                    .withTimestamp(maxTimestamp)
                    .withArgs(msg, ndArrays.size())
                    .buildAndRun(storage);
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        if (newFeature.getElement() == null) return; // Element might be null if not yet arrived
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        for (Edge edge : outEdges) {
            if (messageReady(edge) && edge.getTimestamp() <= last_reduce) {
                if (Objects.isNull(msgOld)) {
                    msgOld = this.message(oldFeature.getValue(), false);
                    msgNew = this.message(newFeature.getValue(), false);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("replace")
                        .hasUpdate()
                        .withTimestamp(newFeature.getTimestamp())
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msgNew, msgOld)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     *
     * @param element Vertex to be initialized
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = BaseNDManager.threadNDManager.get().zeros(inputShape);
            element.setFeature("agg", new MeanAggregator(aggStart, true), storage.layerFunction.currentTimestamp());

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = BaseNDManager.threadNDManager.get().randomNormal(inputShape); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("feature", new Tensor(embeddingRandom), storage.layerFunction.currentTimestamp());
            }
        }
    }

    /**
     * Push the embedding of this vertex to the next layer
     * After first layer, this is only fushed if agg and features are in sync
     *
     * @param v Vertex
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        if (updateReady(v)) {
            NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
            NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
            NDArray update = this.update(ft, agg, false);
            Vertex messageVertex = v.copy();
            long timestamp = Math.max(v.getFeature("agg").getTimestamp(), v.getFeature("feature").getTimestamp());
            messageVertex.setFeature("feature", new Tensor(update), timestamp);
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex, timestamp), MessageDirection.FORWARD);
        }
    }


    // NEURAL NETWORK FUNCTIONS

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature
     * @param agg      Aggregator Feature
     * @param training training enabled
     * @return Next layer feature
     */
    public NDArray update(NDArray feature, NDArray agg, boolean training) {
        return ((GNNBlock) model.getBlock()).getUpdateBlock().forward(this.parameterStore, new NDList(feature, agg), training).get(0);
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param feature  Source Vertex feature
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public NDArray message(NDArray feature, boolean training) {
        return ((GNNBlock) model.getBlock()).getMessageBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
    }

    /**
     * @param edge Edge
     * @return Is the Edge ready to pass on the message
     */
    public boolean messageReady(Edge edge) {
        return Objects.nonNull(edge.src.getFeature("feature"));
    }

    /**
     * @param vertex Vertex
     * @return Is the Vertex ready to be updated
     */
    public boolean updateReady(Vertex vertex) {
        return vertex != null && vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && Objects.nonNull(vertex.getFeature("agg"));
    }
}
