package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.training.ParameterStore;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import operators.coordinators.events.ActionTaken;
import operators.coordinators.events.ElementsSynced;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * For each Edge, Vertex, Feature addition preforms 1 layer of GNN Embedding
 * Outputs -> New Feature to the next layer
 */
public class MixedGNNEmbeddingLayer extends Plugin {

    public transient ParameterStore modelServer;

    public final String modelName;

    public final boolean externalFeatures; // Do we expect external features or have to initialize features on the first layer

    public transient Batchifier batchifier;

    public HashMap<Short, HashMap<String, Edge>> REDUCE_EDGES;

    public HashMap<Short, HashMap<String, Edge>> UPDATE_EDGES;

    public HashMap<Short, HashMap<String, Vertex>> PENDING_VERTICES;

    public MixedGNNEmbeddingLayer(String modelName, boolean externalFeatures) {
        super(String.format("%s-inferencer", modelName));
        this.externalFeatures = externalFeatures;
        this.modelName = modelName;
    }

    @Override
    public void open() {
        super.open();
        modelServer = (ParameterStore) storage.getPlugin(String.format("%s-server", modelName));
        batchifier = new StackBatchifier();
        REDUCE_EDGES = new HashMap<>();
        UPDATE_EDGES = new HashMap<>();
        PENDING_VERTICES = new HashMap<>();
        REDUCE_EDGES.put(masterPart(), new HashMap<>());
        UPDATE_EDGES.put(masterPart(), new HashMap<>());
        PENDING_VERTICES.put(masterPart(), new HashMap<>());
        replicaParts().forEach(partId -> {
            REDUCE_EDGES.put(partId, new HashMap<>());
            UPDATE_EDGES.put(partId, new HashMap<>());
            PENDING_VERTICES.put(partId, new HashMap<>());
        });
    }

    // INITIALIZATION DONE!!!


    @Override
    @SuppressWarnings("all")
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            if (messageReady(edge)) {
                REDUCE_EDGES.get(getPartId()).putIfAbsent(edge.getId(), edge.copy()); // Features will be here
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                collectReduceEdges((Vertex) feature.getElement());
                PENDING_VERTICES.get(getPartId()).putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
            } else if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                PENDING_VERTICES.get(getPartId()).putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
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
                collectUpdateEdges((Vertex) feature.getElement(), (Tensor) oldFeature);
                PENDING_VERTICES.get(getPartId()).putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
            }
            if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                PENDING_VERTICES.get(getPartId()).putIfAbsent(feature.getElement().getId(), ((Vertex) feature.getElement()).copy());
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
            NDArray aggStart = BaseNDManager.threadNDManager.get().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(aggStart, true));

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = BaseNDManager.threadNDManager.get().randomNormal(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("feature", new Tensor(embeddingRandom));
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param v          Vertex to be collected for
     * @param oldFeature Updated old Feature
     */
    public void collectUpdateEdges(Vertex v, Tensor oldFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(v, EdgeType.OUT);
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                Edge tmpCopy = edge.copy();
                tmpCopy.src.setFeature("oldFeature", oldFeature.copy());
                UPDATE_EDGES.get(getPartId()).putIfAbsent(tmpCopy.getId(), tmpCopy);
            }
        }
    }

    /**
     * Collect all the edges available at the given moment as reduce edges
     *
     * @param vertex Vertex to be collected for
     */
    public void collectReduceEdges(Vertex vertex) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                REDUCE_EDGES.get(getPartId()).putIfAbsent(edge.getId(), edge.copy());
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
            NDArray update = this.update(new NDList(ft,agg),false).get(0);
            Vertex messageVertex = v.copy();
            Long timestamp = v.getFeature("agg").getTimestamp();
            messageVertex.setFeature("feature", new Tensor(update), timestamp);
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex), MessageDirection.FORWARD);
        }
    }

    /**
     * For all reducable edges reduce them
     */
    public void reduceAllEdges() {
        // 1. Collect all Source node Features
        HashMap<String, NDList> sourceVertices = new HashMap<>();
        REDUCE_EDGES.get(getPartId()).values().forEach(edge -> {
            edge.setStorage(this.storage);
            sourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("feature").getValue()));
        });
        if (sourceVertices.isEmpty()) return;
        NDList inputs = batchifier.batchify(sourceVertices.values().toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(this.message(inputs, false));
        int i = 0;
        for (String key : sourceVertices.keySet()) {
            sourceVertices.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }
        // 2. Send the messages
        REDUCE_EDGES.get(getPartId()).values().forEach(edge -> {
            new RemoteInvoke()
                    .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(edge.dest.masterPart())
                    .withTimestamp(edge.getTimestamp())
                    .withArgs(sourceVertices.get(edge.src.getId()).get(0), 1)
                    .buildAndRun(storage);
        });
    }

    public void updateAllEdges() {
        HashMap<String, NDList> newSourceVertices = new HashMap<>();
        HashMap<String, NDList> oldSourceVertices = new HashMap<>();
        UPDATE_EDGES.get(getPartId()).values().forEach(edge -> {
            if (REDUCE_EDGES.get(getPartId()).containsKey(edge.getId())) return;
            edge.setStorage(this.storage);
            newSourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("feature").getValue()));
            oldSourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("oldFeature").getValue()));
        });
        if (newSourceVertices.isEmpty()) return;
        List<NDList> allFeatures = new ArrayList<>();
        allFeatures.addAll(newSourceVertices.values());
        allFeatures.addAll(oldSourceVertices.values());
        NDList inputs = batchifier.batchify(allFeatures.toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(message(inputs, false));
        int i = 0;
        for (String key : newSourceVertices.keySet()) {
            oldSourceVertices.put(key, messages[i + oldSourceVertices.size()]);
            newSourceVertices.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }
        UPDATE_EDGES.get(getPartId()).values().forEach(edge -> {
            if (REDUCE_EDGES.get(getPartId()).containsKey(edge.getId())) return;
            new RemoteInvoke()
                    .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("replace")
                    .hasUpdate()
                    .addDestination(edge.dest.masterPart())
                    .withTimestamp(edge.getTimestamp())
                    .withArgs(newSourceVertices.get(edge.src.getId()).get(0), oldSourceVertices.get(edge.src.getId()).get(0))
                    .buildAndRun(storage);
        });

    }

    public void forwardAllVertices() {
        PENDING_VERTICES.get(getPartId()).values().forEach(vertex -> {
            vertex.setStorage(this.storage);
            if (updateReady(vertex)) {
                forward(vertex);
            }
        });
    }

    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        if(event instanceof ElementsSynced){
            reduceAllEdges();
            updateAllEdges();
            REDUCE_EDGES.get(getPartId()).clear();
            UPDATE_EDGES.get(getPartId()).clear();
        }else if(event instanceof ActionTaken){
            forwardAllVertices();
            PENDING_VERTICES.get(getPartId()).clear();
        }
    }



    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature list
     * @param training training enabled
     * @return Next layer feature
     */
    public NDList update(NDList feature,boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getUpdateBlock().forward(modelServer, feature, training);
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param features Source vertex Features or Batch
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public NDList message(NDList features, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer, features, training);
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
