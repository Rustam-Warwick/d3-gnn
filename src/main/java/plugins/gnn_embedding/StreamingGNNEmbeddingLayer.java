package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.enums.Op;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Objects;

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
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.elementType() == ElementType.EDGE) {
            DEdge dEdge = (DEdge) element;
            if (messageReady(dEdge)) {
                NDList msg = MESSAGE(new NDList((NDArray) dEdge.getSrc().getFeature("f").getValue()), false);
                Rmi.buildAndRun(
                        new Rmi(Feature.encodeFeatureId(ElementType.VERTEX, dEdge.getDestId(), "agg"), "reduce", ElementType.ATTACHED_FEATURE, new Object[]{msg, 1}, true), storage,
                        dEdge.getDest().masterPart(),
                        MessageDirection.ITERATE
                );
            }
        } else if (element.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                // No need to check for agg since it is always the second thing that comes
                reduceOutEdges((Vertex) feature.getElement());
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
            if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
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
        NDArray ft = (NDArray) (v.getFeature("f")).getValue();
        NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
        NDArray update = UPDATE(new NDList(ft, agg), false).get(0);
        Tensor tmp = new Tensor("f", update, false, v.masterPart());
        tmp.attachedTo.f0 = ElementType.VERTEX;
        tmp.attachedTo.f1 = v.getId();
        throughput.inc();
        latency.inc(storage.layerFunction.getTimerService().currentProcessingTime() - storage.layerFunction.currentTimestamp());
        storage.layerFunction.message(new GraphOp(Op.COMMIT, tmp.masterPart(), tmp), MessageDirection.FORWARD);
    }

    /**
     * Given vertex reduce all of its out edges
     *
     * @param v Vertex
     */
    public void reduceOutEdges(Vertex v) {
        Iterable<DEdge> outEdges = storage.getIncidentEdges(v, EdgeType.OUT);
        final Object[] msg = new Object[]{null, 1};
        for (DEdge dEdge : outEdges) {
            if (messageReady(dEdge)) {
                if (Objects.isNull(msg[0])) {
                    msg[0] = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                }
                Rmi.buildAndRun(
                        new Rmi(Feature.encodeFeatureId(ElementType.VERTEX, v.getId(), "agg"),
                                "reduce",
                                ElementType.ATTACHED_FEATURE,
                                msg,
                                true
                        ),
                        storage,
                        v.masterPart(),
                        MessageDirection.ITERATE
                );
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<DEdge> outEdges = storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDList[] msgs = new NDList[2];
        for (DEdge dEdge : outEdges) {
            if (messageReady(dEdge)) {
                if (Objects.isNull(msgs[0])) {
                    msgs[0] = MESSAGE(new NDList(newFeature.getValue()), false);
                    msgs[1] = MESSAGE(new NDList(oldFeature.getValue()), false);
                }
                Rmi.buildAndRun(
                        new Rmi(Feature.encodeFeatureId(ElementType.VERTEX, newFeature.attachedTo.f1, "agg"),
                                "replace",
                                ElementType.ATTACHED_FEATURE,
                                msgs,
                                true
                        ),
                        storage,
                        newFeature.masterPart(),
                        MessageDirection.ITERATE
                );
            }
        }

    }

}
