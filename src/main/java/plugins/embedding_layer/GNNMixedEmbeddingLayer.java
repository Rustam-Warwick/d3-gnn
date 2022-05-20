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
import elements.iterations.RemoteInvoke;
import features.Tensor;
import functions.nn.MyParameterStore;

import java.util.HashMap;
import java.util.Objects;

/**
 * For each Edge, Vertex, Feature addition preforms 1 layer of GNN Embedding
 * Outputs -> New Feature to the next layer
 */
public class GNNMixedEmbeddingLayer extends Plugin {
    // ---------------------- MODEL ---------------------

    public Model model; // Model with block of GNNLayer
    public transient Shape inputShape; // InputShape for this model
    public transient MyParameterStore parameterStore;

    // ---------------------- RUNTIME RELATED -------------------

    public boolean externalFeatures; // Do we expect external features or have to initialize features on the first layer
    public HashMap<String,Edge> REDUCE_EDGES;
    public HashMap<String,Edge> UPDATE_EDGES;
    public HashMap<String, Vertex> PENDING_VERTICES;
    public GNNMixedEmbeddingLayer(Model model, boolean externalFeatures) {
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
        REDUCE_EDGES = new HashMap<>();
        UPDATE_EDGES = new HashMap<>();
        PENDING_VERTICES = new HashMap<>();
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
        } else if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            if(messageReady(edge)){
                REDUCE_EDGES.putIfAbsent(edge.getId(), edge.copy()); // Features will be here
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                collectReduceEdges((Tensor) feature);
                PENDING_VERTICES.putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
            } else if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                PENDING_VERTICES.putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                collectUpdateEdges((Tensor) feature, (Tensor) oldFeature);
                PENDING_VERTICES.putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
            }
            if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName()) ) {
                PENDING_VERTICES.putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
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
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void collectUpdateEdges(Tensor newFeature, Tensor oldFeature) {
        if (newFeature.getElement() == null) return; // Element might be null if not yet arrived
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                Edge tmpCopy = edge.copy();
                tmpCopy.setFeature("oldFeature", oldFeature.copy());
                UPDATE_EDGES.putIfAbsent(tmpCopy.getId(), tmpCopy);
            }
        }
    }

    /**
     * Given vertex reduce all the out edges aggregator values
     *
     * @param feature which belong to a feature
     */
    public void collectReduceEdges(Tensor feature) {
        if (feature.getElement() == null) return;
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) feature.getElement(), EdgeType.OUT);
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                REDUCE_EDGES.putIfAbsent(edge.getId(),edge.copy());
            }
        }
    }

    // NN Functions
    /**
     * Push the embedding of this vertex to the next layer
     * After first layer, this is only fushed if agg and features are in sync
     *
     * @param v Vertex
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        if (updateReady(v) && (storage.layerFunction.isFirst() || v.getFeature("feature").getTimestamp() == v.getFeature("agg").getTimestamp())) {
            NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
            NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
            NDArray update = this.update(ft, agg, false);
            Vertex messageVertex = v.copy();
            long timestamp = v.getFeature("agg").getTimestamp();
            messageVertex.setFeature("feature", new Tensor(update), timestamp);
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex, timestamp), MessageDirection.FORWARD);
        }
    }

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
