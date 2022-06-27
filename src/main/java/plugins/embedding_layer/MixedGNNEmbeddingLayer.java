package plugins.embedding_layer;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import operators.events.ActionTaken;
import operators.events.ElementsSynced;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import plugins.ModelServer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * For each Edge, Vertex, Feature addition preforms 1 layer of GNN Embedding
 * Outputs -> New Feature to the next layer
 */
public class MixedGNNEmbeddingLayer extends Plugin {

    public final String modelName; // Model name to identify the ParameterStore
    public final boolean externalFeatures; // Do we expect external features or have to initialize features on the first layer
    public transient ModelServer modelServer; // ParameterServer Plugin
    public transient Batchifier batchifier; // If we process data as batches

    public MixedGNNEmbeddingLayer(String modelName, boolean externalFeatures) {
        super(String.format("%s-inferencer", modelName));
        this.externalFeatures = externalFeatures;
        this.modelName = modelName;
    }

    @Override
    public void open() {
        super.open();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        batchifier = new StackBatchifier();
    }

    // INITIALIZATION DONE!!!

    // Get Features of this plugin that store updates
    public Feature<HashSet<String>, HashSet<String>> getPendingVertices() {
        Feature<HashSet<String>, HashSet<String>> pendingVertices = (Feature<HashSet<String>, HashSet<String>>) getFeature("pendingVertices");
        if (pendingVertices == null) {
            pendingVertices = new Feature<HashSet<String>, HashSet<String>>(new HashSet<String>(), true, (short) 0);
            setFeature("pendingVertices", pendingVertices);
        }
        return pendingVertices;
    }

    public Feature<HashSet<Edge>, HashSet<Edge>> getReduceEdges() {
        Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = (Feature<HashSet<Edge>, HashSet<Edge>>) getFeature("reduceEdges");
        if (reduceEdges == null) {
            reduceEdges = new Feature<HashSet<Edge>, HashSet<Edge>>(new HashSet<Edge>(), true, (short) 0);
            setFeature("reduceEdges", reduceEdges);
        }
        return reduceEdges;
    }

    public Feature<HashSet<Edge>, HashSet<Edge>> getUpdateEdges() {
        Feature<HashSet<Edge>, HashSet<Edge>> updateEdges = (Feature<HashSet<Edge>, HashSet<Edge>>) getFeature("updateEdges");
        if (updateEdges == null) {
            updateEdges = new Feature<HashSet<Edge>, HashSet<Edge>>(new HashSet<Edge>(), true, (short) 0);
            setFeature("updateEdges", updateEdges);
        }
        return updateEdges;
    }

    // Populate the 3 data strcutures before watermark arrives

    @Override
    @SuppressWarnings("all")
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            if (messageReady(edge)) {
                Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = getReduceEdges();
                reduceEdges.getValue().add(edge.copy());
                storage.updateFeature(reduceEdges);
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX && ("agg".equals(feature.getName()) || "feature".equals(feature.getName()))) {
                if ("feature".equals(feature.getName())) collectReduceEdges((Vertex) feature.getElement());
                if (feature.getElement() != null && updateReady((Vertex) feature.getElement())) {
                    Feature<HashSet<String>, HashSet<String>> pendingVertices = getPendingVertices();
                    pendingVertices.getValue().add(feature.getElement().getId());
                    storage.updateFeature(pendingVertices);
                }
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.attachedTo.f0 == ElementType.VERTEX && ("agg".equals(feature.getName()) || "feature".equals(feature.getName()))) {
                if ("feature".equals(feature.getName()))
                    collectUpdateEdges((Vertex) feature.getElement(), (Tensor) oldFeature);
                if (feature.getElement() != null && updateReady((Vertex) feature.getElement())) {
                    Feature<HashSet<String>, HashSet<String>> pendingVertices = getPendingVertices();
                    pendingVertices.getValue().add(feature.getElement().getId());
                    storage.updateFeature(pendingVertices);
                }
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
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(aggStart, true));

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = LifeCycleNDManager.getInstance().randomNormal(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
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
        Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = getReduceEdges();
        Feature<HashSet<Edge>, HashSet<Edge>> updateEdges = getUpdateEdges();
        boolean is_updated = false;
        for (Edge edge : outEdges) {
            if (messageReady(edge) && !reduceEdges.getValue().contains(edge)) {
                is_updated = true;
                Edge tmpCopy = edge.copy();
                tmpCopy.src.setFeature("oldFeature", oldFeature.copy());
                updateEdges.getValue().add(tmpCopy);
            }
        }
        if (is_updated) storage.updateFeature(updateEdges);
    }

    /**
     * Collect all the edges available at the given moment as reduce edges
     *
     * @param vertex Vertex to be collected for
     */
    public void collectReduceEdges(Vertex vertex) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = getReduceEdges();
        boolean is_updated = false;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                is_updated = true;
                reduceEdges.getValue().add(edge.copy());

            }
        }
        if (is_updated) storage.updateFeature(reduceEdges);
    }

    // Methods for doing the REDUCE, UPDATE, FORWARD functions

    /**
     * Push the embedding of this vertex to the next layer
     * After first layer, this is only fushed if agg and features are in sync
     *
     * @param v Vertex
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
        NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
        NDArray update = UPDATE(new NDList(ft, agg), false).get(0);
        Vertex messageVertex = v.copy();
        Long timestamp = v.getFeature("agg").getTimestamp();
        messageVertex.setFeature("feature", new Tensor(update), timestamp);
        storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex), MessageDirection.FORWARD);
    }

    /**
     * For all reducable edges reduce them
     */
    public void reduceCollectedEdges() {
        // 1. Collect all Source node Features
        LinkedHashMap<String, NDList> sourceVertices = new LinkedHashMap<>();
        Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = getReduceEdges();
        reduceEdges.getValue().forEach(edge -> {
            edge.setStorage(this.storage);
            sourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("feature").getValue()));
        });
        if (sourceVertices.isEmpty()) return;
        NDList inputs = batchifier.batchify(sourceVertices.values().toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(MESSAGE(inputs, false));
        int i = 0;
        for (String key : sourceVertices.keySet()) {
            sourceVertices.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }
        // 2. Send the messages
        reduceEdges.getValue().forEach(edge -> {
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
        reduceEdges.getValue().clear();
        storage.updateFeature(reduceEdges);
    }

    /**
     * For all update edges update them
     */
    public void updateCollectedEdges() {
        HashMap<String, NDList> newSourceVertices = new LinkedHashMap<>();
        HashMap<String, NDList> oldSourceVertices = new LinkedHashMap<>();
        Feature<HashSet<Edge>, HashSet<Edge>> updateEdges = getUpdateEdges();
        updateEdges.getValue().forEach(edge -> {
            edge.setStorage(this.storage);
            newSourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("feature").getValue()));
            oldSourceVertices.putIfAbsent(edge.src.getId(), new NDList((NDArray) edge.src.getFeature("oldFeature").getValue()));
        });
        if (newSourceVertices.isEmpty()) return;
        List<NDList> allFeatures = new ArrayList<>();
        allFeatures.addAll(newSourceVertices.values());
        allFeatures.addAll(oldSourceVertices.values());
        NDList inputs = batchifier.batchify(allFeatures.toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(MESSAGE(inputs, false));
        int i = 0;
        for (String key : newSourceVertices.keySet()) {
            oldSourceVertices.put(key, messages[i + oldSourceVertices.size()]);
            newSourceVertices.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }
        updateEdges.getValue().forEach(edge -> {
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
        updateEdges.getValue().clear();
        storage.updateFeature(updateEdges);

    }

    public void forwardCollectedVertices() {
        Feature<HashSet<String>, HashSet<String>> pendingVertices = getPendingVertices();
        pendingVertices.getValue().forEach(vertexId -> {
            Vertex vertex = storage.getVertex(vertexId);
            forward(vertex);
        });
        pendingVertices.getValue().clear();
        storage.updateFeature(pendingVertices);
    }

    // Reinference Related

    public void reInferenceFirstPartStart() {
        // 1. Clear the stored stuff so far since we don't need them anymore
        Feature<HashSet<Edge>, HashSet<Edge>> reduceEdges = getReduceEdges();
        reduceEdges.getValue().clear();
        storage.updateFeature(reduceEdges);
        Feature<HashSet<Edge>, HashSet<Edge>> updateEdges = getUpdateEdges();
        updateEdges.getValue().clear();
        storage.updateFeature(updateEdges);
        Feature<HashSet<String>, HashSet<String>> pendingFeatures = getPendingVertices();
        pendingFeatures.getValue().clear();
        storage.updateFeature(pendingFeatures);

        // 2. Clear Aggregators + InReduce all the existing edges
        HashMap<Vertex, List<String>> inEdges = new HashMap<>();
        LinkedHashMap<String, NDList> featureMap = new LinkedHashMap<>();
        for (Vertex v : storage.getVertices()) {
            // 1. Clear the aggregators
            if (v.getFeature("agg") != null) {
                ((BaseAggregator<?>) v.getFeature("agg")).reset();
                v.getFeature("agg").update(v.getFeature("agg"));
            }

            Iterable<Edge> localInEdges = storage.getIncidentEdges(v, EdgeType.IN);
            List<String> tmp = new ArrayList<>();
            for (Edge localInEdge : localInEdges) {
                if (featureMap.containsKey(localInEdge.src.getId()) || messageReady(localInEdge)) {
                    tmp.add(localInEdge.src.getId());
                    featureMap.putIfAbsent(localInEdge.src.getId(), new NDList((NDArray) localInEdge.src.getFeature("feature").getValue()));
                }
            }
            if (!tmp.isEmpty()) inEdges.put(v, tmp);
        }
        NDList inputs = batchifier.batchify(featureMap.values().toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(MESSAGE(inputs, false));
        int i = 0;
        for (String key : featureMap.keySet()) {
            featureMap.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }

        for (Map.Entry<Vertex, List<String>> v : inEdges.entrySet()) {
            List<NDArray> inFeatures = v.getValue().stream().map(item -> featureMap.get(item).get(0)).collect(Collectors.toList());
            NDArray message = MeanAggregator.bulkReduce(inFeatures.toArray(new NDArray[0]));
            new RemoteInvoke()
                    .toElement(v.getKey().decodeFeatureId("agg"), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(v.getKey().masterPart())
                    .withArgs(message, inFeatures.size())
                    .buildAndRun(storage);
        }

    }

    public void reInferenceSecondPartStart() {
        Feature<HashSet<String>, HashSet<String>> pendingFeatures = getPendingVertices();
        pendingFeatures.getValue().clear();
        storage.updateFeature(pendingFeatures);

        for (Vertex v : storage.getVertices()) {
            if (updateReady(v)) {
                forward(v);
            }
        }
        if (isLastReplica()) modelServer.IS_DIRTY = false;
    }

    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof ElementsSynced) {
            if (modelServer.IS_DIRTY) {
                reInferenceFirstPartStart();
            } else {
                reduceCollectedEdges();
                updateCollectedEdges();
            }
        } else if (event instanceof ActionTaken) {
            if (modelServer.IS_DIRTY) {
                reInferenceSecondPartStart();
            } else {
                forwardCollectedVertices();
            }
        }
    }

    // HELPER METHODS for doing the UPDATE and MESSAGE functions, interacting with the model server

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
