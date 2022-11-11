package plugins.hgnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.ReplicaState;
import features.MeanAggregator;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;


/**
 * Base plugin for streaming hypergraph models
 * Following Message -> Aggregate -> Aggregate -> Update Cycles
 */
public class StreamingHGNNEmbeddingLayer extends BaseHGNNEmbeddingPlugin {

    protected transient Counter throughput;

    protected transient Counter latency;

    public StreamingHGNNEmbeddingLayer(String modelName) {
        super(modelName, "inferencer");
    }

    public StreamingHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "inferencer", trainableVertexEmbeddings);
    }

    public StreamingHGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        super(modelName, "inferencer", trainableVertexEmbeddings, IS_ACTIVE);
    }

    @Override
    public void open() throws Exception {
        super.open();
        throughput = new SimpleCounter();
        latency = new MovingAverageCounter(1000);
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        storage.layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
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
            reduceF1(edge); // Reduce from vertices to this hyper-edge
        } else if (element.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.getName().equals("f") && feature.attachedTo.f0 == ElementType.VERTEX) {
                reduceF1((Tensor) feature); // Reduce to hyper-edges for the given vertex feature (F1)
                if (feature.state() == ReplicaState.MASTER) ; // @todo add forward
            } else if (feature.getName().equals("agg") && feature.attachedTo.f0 == ElementType.HYPEREDGE) {
                reduceF2((MeanAggregator) feature); // Reduce all vertex messages
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.HYPEREDGE) {
            HEdge newEdge = (HEdge) newElement;
            HEdge oldEdge = (HEdge) oldElement;
            if (newEdge.getVertexIds().size() != oldEdge.getVertexIds().size())
                reduceF1(newEdge, oldEdge); // Reduce from newly arrived vertices (F1)
        } else if (newElement.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (newFeature.getName().equals("f") && newFeature.attachedTo.f0 == ElementType.VERTEX) {
                replaceF1((Tensor) newFeature, (Tensor) oldFeature); // Replace previously reduced hyper-edges (F1)
                // @todo Forward
            } else if (newFeature.getName().equals("agg") && newFeature.attachedTo.f0 == ElementType.HYPEREDGE) {
                replaceF2((MeanAggregator) newFeature, (MeanAggregator) oldFeature); // Replace previously reduced vertices (F2)
            } else if (newFeature.getName().equals("agg") && newFeature.attachedTo.f0 == ElementType.VERTEX) {
                // @todo Forward
            }
        }
    }

    /**
     * Reduce from vertex to local hyper-edges (F1)
     */
    public void reduceF1(Tensor f) {
        NDList message = null;
        Vertex v = (Vertex) f.getElement();
        for (HEdge hyperEdge : storage.getIncidentHyperEdges(v)) {
            if (message == null) message = MESSAGE(new NDList(f.getValue()), false);
            Rmi.buildAndRun(
                    new Rmi(
                            Feature.encodeFeatureId(ElementType.HYPEREDGE, hyperEdge.getId(), "agg"),
                            "reduce",
                            ElementType.ATTACHED_FEATURE,
                            new Object[]{message, 1},
                            true
                    ),
                    storage,
                    hyperEdge.masterPart(),
                    MessageDirection.ITERATE
            );
        }
    }

    /**
     * Reduce from new vertices added to hyper-edge (F1)
     */
    public void reduceF1(HEdge newEdge, HEdge oldEdge) {
        for (int i = oldEdge.getVertexIds().size(); i < newEdge.vertexIds.size(); i++) {
            Vertex vertex = newEdge.getVertex(i);
            if (messageReady(vertex)) {
                NDList message = MESSAGE(new NDList((NDArray) vertex.getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        new Rmi(
                                Feature.encodeFeatureId(ElementType.HYPEREDGE, newEdge.getId(), "agg"),
                                "reduce",
                                ElementType.ATTACHED_FEATURE,
                                new Object[]{message, 1},
                                true
                        ),
                        storage,
                        newEdge.masterPart(),
                        MessageDirection.ITERATE
                );
            }
        }
    }

    /**
     * Reduce all the local vertices to the given Hyper-edge (F1)
     */
    public void reduceF1(HEdge edge) {
        for (Vertex vertex : edge.getVertices()) {
            if (messageReady(vertex)) {
                NDList message = MESSAGE(new NDList((NDArray) vertex.getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        new Rmi(
                                Feature.encodeFeatureId(ElementType.HYPEREDGE, edge.getId(), "agg"),
                                "reduce",
                                ElementType.ATTACHED_FEATURE,
                                new Object[]{message, 1},
                                true
                        ),
                        storage,
                        edge.masterPart(),
                        MessageDirection.ITERATE
                );
            }
        }
    }

    /**
     * Reduce Hedge to all its vertices (F2)
     */
    public void reduceF2(MeanAggregator aggregator) {
        HEdge edge = (HEdge) aggregator.getElement();
        NDList message = new NDList(aggregator.getValue());
        for (Vertex vertex : edge.getVertices()) {
            Rmi.buildAndRun(
                    new Rmi(
                            Feature.encodeFeatureId(ElementType.VERTEX, vertex.getId(), "agg"),
                            "reduce",
                            ElementType.ATTACHED_FEATURE,
                            new Object[]{message, 1},
                            true
                    ),
                    storage,
                    vertex.masterPart(),
                    MessageDirection.ITERATE
            );
        }
    }

    /**
     * Update HyperEdges if the initial message function happens to be changed
     */
    public void replaceF1(Tensor newFeature, Tensor oldFeature) {
        NDList newMessage = MESSAGE(new NDList(newFeature.getValue()), false);
        NDList oldMessage = MESSAGE(new NDList(oldFeature.getValue()), false);
        for (HEdge hyperEdge : storage.getIncidentHyperEdges((Vertex) newFeature.getElement())) {
            Rmi.buildAndRun(
                    new Rmi(
                            Feature.encodeFeatureId(ElementType.HYPEREDGE, hyperEdge.getId(), "agg"),
                            "replace",
                            ElementType.ATTACHED_FEATURE,
                            new Object[]{newMessage, oldMessage},
                            true
                    ),
                    storage,
                    hyperEdge.masterPart(),
                    MessageDirection.ITERATE
            );
        }
    }

    /**
     * Update Vertices when the HyperEdge aggregator is updated
     */
    public void replaceF2(MeanAggregator newAggregator, MeanAggregator oldAggregator) {
        NDList newMessage = MESSAGE(new NDList(newAggregator.getValue()), false);
        NDList oldMessage = MESSAGE(new NDList(oldAggregator.getValue()), false);
    }


}
