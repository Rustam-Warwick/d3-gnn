package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.HGNNBlock;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import plugins.ModelServer;


/**
 * Base plugin for streaming hypergraph models
 * Following Message -> Aggregate -> Aggregate -> Update Cycles
 */
public class StreamingHGNNEmbeddingLayer extends Plugin {

    public final String modelName;

    public final Boolean createVertexEmbedding;

    protected transient ModelServer modelServer; // ParameterServer Plugin

    protected transient Counter throughput;

    protected transient Counter latency;

    public StreamingHGNNEmbeddingLayer(String modelName) {
        this(modelName, false);
    }

    public StreamingHGNNEmbeddingLayer(String modelName, boolean createVertexEmbeddings) {
        super(String.format("%s-inferencer", modelName));
        this.modelName = modelName;
        this.createVertexEmbedding = createVertexEmbeddings;
    }

    @Override
    public void open() throws Exception {
        super.open();
        assert storage != null;
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        throughput = new SimpleCounter();
        latency = new MovingAverageCounter(1000);
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        storage.layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
    }

    public void initHyperEdge(HEdge edge) {
        if (edge.state() == ReplicaState.MASTER) {
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getInputShape().get(0).getValue());
            edge.setFeature("agg", new MeanAggregator(aggStart, false));
        }
    }

    public void initVertex(Vertex vertex) {
        if (vertex.state() == ReplicaState.MASTER) {
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getInputShape().get(0).getValue());
            vertex.setFeature("agg", new MeanAggregator(aggStart, true));
            if (createVertexEmbedding && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = LifeCycleNDManager.getInstance().ones(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                vertex.setFeature("f", new Tensor(embeddingRandom));
            }
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            Vertex vertex = (Vertex) element;
            initVertex(vertex); // Initialize the feature and aggregators
        } else if (element.elementType() == ElementType.HYPEREDGE) {
            HEdge edge = (HEdge) element;
            initHyperEdge(edge); // Initialize the aggregator for the hyper-edges
            for (Vertex vertex : edge.getVertices()) {
                // Reduce the vertices of the given hyper-edge (F1)
                if (messageReady(vertex)) {
                    NDList message = MESSAGE(new NDList((NDArray) vertex.getFeature("f").getValue()), false);
                    new RemoteInvoke()
                            .toElement(Feature.encodeAttachedFeatureId("agg", edge.getId(), ElementType.HYPEREDGE), ElementType.FEATURE)
                            .where(MessageDirection.ITERATE)
                            .method("reduce")
                            .hasUpdate()
                            .addDestination(edge.masterPart())
                            .withArgs(message, 1)
                            .buildAndRun(storage);
                }
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.getName().equals("f") && feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX) {
                reduceHyperEdges((Vertex) feature.getElement()); // Reduce all hyper-edges for the given vertex feature (F1)
                // Forward (Update)
            } else if (feature.getName().equals("agg") && feature.attachedTo != null && feature.attachedTo.f0 == ElementType.HYPEREDGE) {
                // HyperEdge aggregator was created so reduce all vertex messages (F2)
                HEdge edge = (HEdge) feature.getElement();
                NDList message = new NDList((NDArray) feature.getValue());
                for (Vertex vertex : edge.getVertices()) {
                    new RemoteInvoke()
                            .toElement(Feature.encodeAttachedFeatureId("agg", vertex.getId(), ElementType.VERTEX), ElementType.FEATURE)
                            .where(MessageDirection.ITERATE)
                            .method("reduce")
                            .hasUpdate()
                            .addDestination(vertex.masterPart())
                            .withArgs(message, 1)
                            .buildAndRun(storage);
                }
            } else if (feature.getName().equals("agg") && feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX) {
                // Forward (Update)
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.HYPEREDGE) {
            // Update in hyperedge either new vertices added or some Feature update
            HEdge newEdge = (HEdge) newElement;
            HEdge oldEdge = (HEdge) oldElement;
            // 2. Check if new vertex was added to this hyperedge then reduce that vertex
            if (newEdge.vertexIds.size() != oldEdge.vertexIds.size()) {
                // There was an addition of vertex ids
                for (String vertexId : newEdge.vertexIds) {
                    if (!oldEdge.vertexIds.contains(vertexId)) {
                        // This is the new one reduce the new vertices as well
                        Vertex v = storage.getVertex(vertexId);
                        if (messageReady(v)) {
                            NDList message = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                            new RemoteInvoke()
                                    .toElement(Feature.encodeAttachedFeatureId("agg", newEdge.getId(), ElementType.HYPEREDGE), ElementType.FEATURE)
                                    .where(MessageDirection.ITERATE)
                                    .method("reduce")
                                    .hasUpdate()
                                    .addDestination(newEdge.masterPart())
                                    .withArgs(message, 1)
                                    .buildAndRun(storage);
                        }
                    }
                }
            }
            // 3. Check if HyperEdge feature was changed (NOT YET IMPLEMENTED)
        } else if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (newFeature.getName().equals("f") && newFeature.attachedTo != null && newFeature.attachedTo.f0 == ElementType.VERTEX) {
                // Vertex Feature update re-calculate hyper-edge aggregators (F1)
                updateHyperEdges((Tensor) newFeature, (Tensor) oldFeature);
                // Forward (Update)
            } else if (newFeature.getName().equals("agg") && newFeature.attachedTo != null && newFeature.attachedTo.f0 == ElementType.HYPEREDGE) {
                // HyperEdge Feature update, re-calculate Vertex aggregators (F2)
            } else if (newFeature.getName().equals("agg") && newFeature.attachedTo != null && newFeature.attachedTo.f0 == ElementType.VERTEX) {
                // Vertex Aggregator update, Forward (Update)
            }

        }

    }

    /**
     * Reduce HyperEdges for a given Vertex
     * Only happns if vertex was replicated and feature arrives later or in subsequent HGNN layers
     */
    public void reduceHyperEdges(Vertex v) {
        if (!messageReady(v)) return;
        NDList message = null;
        for (HEdge hyperEdge : storage.getHyperEdges(v)) {
            if (message == null) message = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("agg", hyperEdge.getId(), ElementType.HYPEREDGE), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(hyperEdge.masterPart())
                    .withArgs(message, 1)
                    .buildAndRun(storage);
        }
    }

    /**
     * Update HyperEdges if the initial message function happens to be changed
     */
    public void updateHyperEdges(Tensor newFeature, Tensor oldFeature) {
        NDList newMessage = MESSAGE(new NDList(newFeature.getValue()), false);
        NDList oldMessage = MESSAGE(new NDList(oldFeature.getValue()), false);
        for (HEdge hyperEdge : storage.getHyperEdges((Vertex) newFeature.getElement())) {
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("agg", hyperEdge.getId(), ElementType.HYPEREDGE), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("replace")
                    .hasUpdate()
                    .addDestination(hyperEdge.masterPart())
                    .withArgs(newMessage, oldMessage)
                    .buildAndRun(storage);
        }
    }

    /**
     * Is the vertex ready to generate a message
     */
    public boolean messageReady(Vertex v) {
        return v != null && v.containsFeature("f");
    }

    /**
     * Get the message of a given vertex
     */
    public NDList MESSAGE(NDList features, boolean training) {
        return ((HGNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), features, training);
    }


}
