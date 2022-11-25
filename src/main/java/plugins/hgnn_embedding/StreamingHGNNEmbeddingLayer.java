package plugins.hgnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
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
        getStorage().layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        getStorage().layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the feature and aggregators
        } else if (element.getType() == ElementType.HYPEREDGE) {
            HyperEdge edge = (HyperEdge) element;
            initHyperEdge(edge); // Initialize the aggregator for the hyper-edges
            reduceF1(edge); // Reduce from vertices to this hyper-edge
        } else if (element.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if ("f".equals(feature.getName()) && feature.ids.f0 == ElementType.VERTEX) {
                reduceF1((Tensor) feature); // Reduce to hyper-edges for the given vertex feature (F1)
                if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
            } else if ("agg".equals(feature.getName()) && feature.ids.f0 == ElementType.HYPEREDGE) {
                reduceF2((MeanAggregator) feature); // Reduce all vertex messages
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.HYPEREDGE) {
            HyperEdge newEdge = (HyperEdge) newElement;
            HyperEdge oldEdge = (HyperEdge) oldElement;
            if (oldEdge.getVertexIds().size() > 0)
                partialReduceF1andF2(newEdge, oldEdge); // Reduce from newly arrived vertices (F1)
        } else if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (newFeature.getName().equals("f") && newFeature.ids.f0 == ElementType.VERTEX) {
                replaceF1((Tensor) newFeature, (Tensor) oldFeature); // Replace previously reduced hyper-edges (F1)
                if (newFeature.state() == ReplicaState.MASTER) forward((Vertex) newFeature.getElement());
            } else if (newFeature.getName().equals("agg") && newFeature.ids.f0 == ElementType.HYPEREDGE) {
                replaceF2((MeanAggregator) newFeature, (MeanAggregator) oldFeature); // Replace previously reduced vertices (F2)
            } else if (newFeature.getName().equals("agg") && newFeature.ids.f0 == ElementType.VERTEX) {
                if (newFeature.state() == ReplicaState.MASTER && newFeature.getElement().containsFeature("f"))
                    forward((Vertex) newFeature.getElement());
            }
        }
    }

    /**
     * Push the embedding of this vertex to the next layer
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        NDArray ft = (NDArray) (v.getFeature("f")).getValue();
        NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
        NDArray update = UPDATE(new NDList(ft, agg), false).get(0);
        Tensor tmp = new Tensor("f", update, false, v.getMasterPart());
        tmp.ids.f0 = ElementType.VERTEX;
        tmp.ids.f1 = v.getId();
        throughput.inc();
        latency.inc(getStorage().layerFunction.getTimerService().currentProcessingTime() - getStorage().layerFunction.currentTimestamp());
        getStorage().layerFunction.message(new GraphOp(Op.COMMIT, v.getMasterPart(), tmp), MessageDirection.FORWARD);
    }

    /**
     * Reduce from vertex to local hyper-edges (F1)
     */
    public void reduceF1(Tensor f) {
        NDList message = null;
        Vertex v = (Vertex) f.getElement();
        for (HyperEdge hyperEdge : getStorage().getIncidentHyperEdges(v)) {
            if (message == null) message = MESSAGE(new NDList(f.getValue()), false);
            Rmi.buildAndRun(
                    Feature.encodeFeatureId(ElementType.HYPEREDGE, hyperEdge.getId(), "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "reduce",
                    hyperEdge.getMasterPart(),
                    MessageDirection.ITERATE,
                    message,
                    1
            );
        }
    }

    /**
     * Reduce from new vertices added to hyper-edge (F1)
     */
    public void partialReduceF1andF2(HyperEdge newEdge, HyperEdge oldEdge) {
        NDList f2Message = newEdge.containsFeature("agg") ? new NDList((NDArray) newEdge.getFeature("agg").getValue()) : null;
        for (int i = newEdge.getVertexIds().size() - oldEdge.getVertexIds().size(); i < newEdge.getVertexIds().size(); i++) {
            Vertex vertex = newEdge.getVertex(i);
            if (messageReady(vertex)) {
                NDList f1Message = MESSAGE(new NDList((NDArray) vertex.getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.HYPEREDGE, newEdge.getId(), "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        newEdge.getMasterPart(),
                        MessageDirection.ITERATE,
                        f1Message,
                        1
                );
            }
            if (f2Message != null) {
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.VERTEX, vertex.getId(), "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        vertex.getMasterPart(),
                        MessageDirection.ITERATE,
                        f2Message,
                        1
                );
            }
        }
    }

    /**
     * Reduce all the local vertices to the given Hyper-edge (F1)
     */
    public void reduceF1(HyperEdge edge) {
        for (Vertex vertex : edge.getVertices()) {
            if (messageReady(vertex)) {
                NDList message = MESSAGE(new NDList((NDArray) vertex.getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.HYPEREDGE, edge.getId(), "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        edge.getMasterPart(),
                        MessageDirection.ITERATE,
                        message,
                        1
                );
            }
        }
    }

    /**
     * Reduce Hedge to all its vertices (F2)
     */
    public void reduceF2(MeanAggregator aggregator) {
        HyperEdge edge = (HyperEdge) aggregator.getElement();
        NDList message = new NDList(aggregator.getValue());
        for (Vertex vertex : edge.getVertices()) {
            Rmi.buildAndRun(
                    Feature.encodeFeatureId(ElementType.VERTEX, vertex.getId(), "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "reduce",
                    vertex.getMasterPart(),
                    MessageDirection.ITERATE,
                    message,
                    1
            );
        }
    }

    /**
     * Update HyperEdges if the initial message function happens to be changed
     */
    public void replaceF1(Tensor newFeature, Tensor oldFeature) {
        NDList newMessage = MESSAGE(new NDList(newFeature.getValue()), false);
        NDList oldMessage = MESSAGE(new NDList(oldFeature.getValue()), false);
        for (HyperEdge hyperEdge : getStorage().getIncidentHyperEdges((Vertex) newFeature.getElement())) {
            Rmi.buildAndRun(
                    Feature.encodeFeatureId(ElementType.HYPEREDGE, hyperEdge.getId(), "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "replace",
                    hyperEdge.getMasterPart(),
                    MessageDirection.ITERATE,
                    newMessage,
                    oldMessage
            );
        }
    }

    /**
     * Update Vertices when the HyperEdge aggregator is updated
     */
    public void replaceF2(MeanAggregator newAggregator, MeanAggregator oldAggregator) {
        NDList newMessage = MESSAGE(new NDList(newAggregator.getValue()), false);
        NDList oldMessage = MESSAGE(new NDList(oldAggregator.getValue()), false);
        HyperEdge edge = (HyperEdge) newAggregator.getElement();
        for (Vertex vertex : edge.getVertices()) {
            Rmi.buildAndRun(
                    Feature.encodeFeatureId(ElementType.VERTEX, vertex.getId(), "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "replace",
                    vertex.getMasterPart(),
                    MessageDirection.ITERATE,
                    newMessage,
                    oldMessage
            );
        }
    }

}
