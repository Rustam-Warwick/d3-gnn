package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.Rmi;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;

import java.util.*;

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
        assert storage != null;
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
                        storage,
                        dEdge.getDest().masterPart(),
                        new Rmi(Feature.encodeFeatureId("agg", dEdge.getDestId(), ElementType.VERTEX), "reduce", ElementType.ATTACHED_FEATURE, new Object[]{msg, 1}, true),
                        MessageDirection.ITERATE
                );
            }
        } else if (element.elementType() == ElementType.ATTACHED_FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                reduceOutEdges((Vertex) feature.getElement());
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            } else if (feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
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
        final NDList[] msg = new NDList[1];
        HashMap<Short, Tuple2<List<String>, NDList>> reduceMessages = null;
        for (DEdge dEdge : outEdges) {
            if (this.messageReady(dEdge)) {
                if (Objects.isNull(msg[0])) {
                    msg[0] = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                    reduceMessages = new HashMap<>();
                }
                reduceMessages.computeIfAbsent(dEdge.getDest().masterPart(), item -> new Tuple2<>(new ArrayList<>(), msg[0]));
                reduceMessages.get(dEdge.getDest().masterPart()).f0.add(dEdge.getDest().getId());
            }
        }
        if (reduceMessages == null) return;
        for (Map.Entry<Short, Tuple2<List<String>, NDList>> shortTuple2Entry : reduceMessages.entrySet()) {
            Rmi.buildAndRun(
                    storage,
                    shortTuple2Entry.getKey(),
                    new Rmi(getId(), "receiveReduceOutEdges", elementType(), new Object[]{shortTuple2Entry.getValue().f0, shortTuple2Entry.getValue().f1}, false),
                    MessageDirection.ITERATE
            );
        }
    }

    @RemoteFunction
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        Rmi rmi = new Rmi(null, "reduce", null, new Object[]{message, 1}, true);
        for (String vertex : vertices) {
            Rmi.execute(storage.getVertex(vertex).getFeature("agg"), rmi);
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
        NDList[] msgOld = new NDList[1];
        NDList[] msgNew = new NDList[1];
        HashMap<Short, Tuple3<List<String>, NDList, NDList>> replaceMessages = null;
        for (DEdge dEdge : outEdges) {
            if (this.messageReady(dEdge)) {
                if (Objects.isNull(msgOld[0])) {
                    msgOld[0] = MESSAGE(new NDList(oldFeature.getValue()), false);
                    msgNew[0] = MESSAGE(new NDList(newFeature.getValue()), false);
                    replaceMessages = new HashMap<>();
                }
                replaceMessages.computeIfAbsent(dEdge.getDest().masterPart(), item -> new Tuple3<>(new ArrayList<>(), msgNew[0], msgOld[0]));
                replaceMessages.get(dEdge.getDest().masterPart()).f0.add(dEdge.getDest().getId());
            }
        }

        if (replaceMessages == null) return;
        for (Map.Entry<Short, Tuple3<List<String>, NDList, NDList>> shortTuple2Entry : replaceMessages.entrySet()) {
            Rmi.buildAndRun(
                    storage,
                    shortTuple2Entry.getKey(),
                    new Rmi(getId(), "receiveReplaceOutEdges", elementType(), new Object[]{shortTuple2Entry.getValue().f0, shortTuple2Entry.getValue().f1, shortTuple2Entry.getValue().f2}, false),
                    MessageDirection.ITERATE
            );
        }
    }

    @RemoteFunction
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        Rmi rmi = new Rmi(null, "replace", null, new Object[]{messageNew, messageOld}, true);
        for (String vertex : vertices) {
            Rmi.execute(storage.getVertex(vertex).getFeature("agg"), rmi);
        }
    }
}