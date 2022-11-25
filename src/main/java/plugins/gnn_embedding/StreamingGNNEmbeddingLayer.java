package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.enums.*;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Objects;

/**
 * {@inheritDoc}
 * Each triggerUpdate produces a new cascading effect and no optimizations are happening
 */
public class StreamingGNNEmbeddingLayer extends BaseGNNEmbeddingPlugin {

    protected transient Counter throughput; // Throughput counter, only used for last layer

    protected transient Counter latency; // Throughput counter, only used for last layer

    public StreamingGNNEmbeddingLayer(String modelName) {
        super(modelName, "inferencer");
    }

    public StreamingGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "inferencer", trainableVertexEmbeddings);
    }

    public StreamingGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        super(modelName, "inferencer", trainableVertexEmbeddings, IS_ACTIVE);
    }

    public StreamingGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean requiresDestForMessage, boolean IS_ACTIVE) {
        super(modelName, "inferencer", trainableVertexEmbeddings, requiresDestForMessage, IS_ACTIVE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws Exception {
        super.open();
        throughput = new SimpleCounter();
        latency = new MovingAverageCounter(1000);
        getStorage().layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        getStorage().layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.getType() == ElementType.EDGE) {
            DirectedEdge directedEdge = (DirectedEdge) element;
            if (messageReady(directedEdge)) {
                NDList msg = MESSAGE(new NDList((NDArray) directedEdge.getSrc().getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.VERTEX, directedEdge.getDestId(), "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        directedEdge.getDest().getMasterPart(),
                        MessageDirection.ITERATE,
                        msg,
                        1
                );
            }
        } else if (element.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if ("f".equals(feature.getName()) && feature.ids.f0 == ElementType.VERTEX) {
                // Feature is always second in creation because aggregators get created immediately after VERTEX
                reduceOutEdges((Vertex) feature.getElement());
                if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.ids.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
            }
            if (feature.ids.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                if (feature.state() == ReplicaState.MASTER && feature.getElement().containsFeature("f"))
                    forward((Vertex) feature.getElement());
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
        Tensor tmp = new Tensor("f", update, false);
        tmp.ids.f0 = ElementType.VERTEX;
        tmp.ids.f1 = v.getId();
        throughput.inc();
        latency.inc(getStorage().layerFunction.getTimerService().currentProcessingTime() - getStorage().layerFunction.currentTimestamp());
        getStorage().layerFunction.message(new GraphOp(Op.COMMIT, v.getMasterPart(), tmp), MessageDirection.FORWARD);
    }

    /**
     * Given vertex reduce all of its out edges
     *
     * @param v Vertex
     */
    public void reduceOutEdges(Vertex v) {
        Iterable<DirectedEdge> outEdges = getStorage().getIncidentEdges(v, EdgeType.OUT);
        final Object[] msg = new Object[]{null, 1};
        for (DirectedEdge directedEdge : outEdges) {
            if (messageReady(directedEdge)) {
                if (Objects.isNull(msg[0])) {
                    msg[0] = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                }
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.VERTEX, v.getId(), "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        v.getMasterPart(),
                        MessageDirection.ITERATE,
                        msg[0],
                        msg[1]
                );
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value triggerUpdate the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<DirectedEdge> outEdges = getStorage().getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDList[] msgs = new NDList[2];
        for (DirectedEdge directedEdge : outEdges) {
            if (messageReady(directedEdge)) {
                if (Objects.isNull(msgs[0])) {
                    msgs[0] = MESSAGE(new NDList(newFeature.getValue()), false);
                    msgs[1] = MESSAGE(new NDList(oldFeature.getValue()), false);
                }
                Rmi.buildAndRun(
                        Feature.encodeFeatureId(ElementType.VERTEX, newFeature.ids.f1, "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "replace",
                        newFeature.getMasterPart(),
                        MessageDirection.ITERATE,
                        msgs[0],
                        msgs[1]
                );
            }
        }

    }

}
