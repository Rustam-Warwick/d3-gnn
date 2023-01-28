package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import elements.features.Tensor;
import functions.metrics.MovingAverageCounter;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import storage.BaseStorage;

import java.util.*;

/**
 * {@inheritDoc}
 */
public class StreamingGNNEmbedding extends BaseGNNEmbedding {

    protected transient Counter throughput; // Throughput counter, only used for last layer

    protected transient Counter latency; // Throughput counter, only used for last layer

    protected transient NDList reuseNDList; // Reusable NDList object for performance

    protected transient Tuple3<ElementType, Object, String> reuseFeatureId; // Reusable Feature id tuple

    protected transient Tensor reuseTensor; // Reuse tensor for forward message

    protected transient Map<Short, List<String>> reuseReduceMap; // Reusable map for sending reduce, replace messages

    public StreamingGNNEmbedding(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "inferencer", trainableVertexEmbeddings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        throughput = new SimpleCounter();
        latency = new MovingAverageCounter(5000);
        getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        getRuntimeContext().getMetricGroup().counter("latency", latency);
        reuseNDList = new NDList(2);
        reuseFeatureId = Tuple3.of(ElementType.VERTEX, null, "agg");
        reuseTensor = new Tensor("f", null, false);
        reuseTensor.id.f0 = ElementType.VERTEX;
        reuseReduceMap = new Short2ObjectOpenHashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            if (element.getType() == ElementType.VERTEX) {
                initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
            } else if (element.getType() == ElementType.EDGE) {
                DirectedEdge directedEdge = (DirectedEdge) element;
                if (messageReady(directedEdge)) {
                    reuseNDList.add((NDArray) directedEdge.getSrc().getFeature("f").getValue());
                    NDList msg = MESSAGE(reuseNDList, false);
                    reuseNDList.clear();
                    reuseFeatureId.f1 = directedEdge.getDestId();
                    Rmi.buildAndRun(
                            reuseFeatureId,
                            ElementType.ATTACHED_FEATURE,
                            "reduce",
                            directedEdge.getDest().getMasterPart(),
                            OutputTags.ITERATE_OUTPUT_TAG,
                            msg,
                            1
                    );
                }
            } else if (element.getType() == ElementType.ATTACHED_FEATURE) {
                Feature<?, ?> feature = (Feature<?, ?>) element;
                if ("f".equals(feature.getName()) && feature.id.f0 == ElementType.VERTEX) {
                    // Feature is always second in creation because aggregators get created immediately after VERTEX
                    reduceOutEdges((Vertex) feature.getElement());
                    if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            if (newElement.getType() == ElementType.ATTACHED_FEATURE) {
                Feature<?, ?> feature = (Feature<?, ?>) newElement;
                Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
                if (feature.id.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                    updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                    if(!feature.getElement().getId().equals(feature.getAttachedElementId())) System.out.println("AAAA");
                    if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
                }
                if (feature.id.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                    if(!feature.getElement().getId().equals(feature.getAttachedElementId())) System.out.println("AAAA");
                    if (feature.state() == ReplicaState.MASTER && feature.getElement().containsFeature("f"))
                        forward((Vertex) feature.getElement());
                }
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
        reuseNDList.add(ft);
        reuseNDList.add(agg);
        NDArray update = UPDATE(reuseNDList, false).get(0);
        reuseNDList.clear();
        reuseTensor.id.f1 = v.getId();
        reuseTensor.value = update;
        getRuntimeContext().output(new GraphOp(Op.UPDATE, v.getMasterPart(), reuseTensor));
        throughput.inc();
        latency.inc(getRuntimeContext().getTimerService().currentProcessingTime() - getRuntimeContext().currentTimestamp());
    }

    /**
     * Given vertex reduce all of its out edges
     * <p>
     *     Now we are assuming that the vertex function only depends on the source "f"
     *     Hence we can optimize by sending single tensor to the parts
     * </p>
     */
    public void reduceOutEdges(Vertex v) {
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.OUT);
            NDList result = null;
            reuseReduceMap.clear();
            for (DirectedEdge directedEdge : outEdges) {
                if (result == null) {
                    reuseNDList.add((NDArray) v.getFeature("f").getValue());
                    result = MESSAGE(reuseNDList, false);
                    reuseNDList.clear();
                }
                reuseReduceMap.computeIfAbsent(directedEdge.getDest().getMasterPart(), item -> new ArrayList<>(4));
                reuseReduceMap.get(directedEdge.getDest().getMasterPart()).add(directedEdge.getDest().getId());
            }
            if (reuseReduceMap.isEmpty()) return;
            for (Map.Entry<Short, List<String>> shortTuple2Entry : reuseReduceMap.entrySet()) {
                Rmi.buildAndRun(
                        getId(),
                        getType(),
                        "receiveReduceOutEdges",
                        shortTuple2Entry.getKey(),
                        OutputTags.ITERATE_OUTPUT_TAG,
                        shortTuple2Entry.getValue(),
                        result
                );
            }
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            for (String vertexId : vertices) {
                reuseFeatureId.f1 = vertexId;
                Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(reuseFeatureId), "reduce", message, 1);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
            NDList msgOld = null;
            NDList msgNew = null;
            reuseReduceMap.clear();
            for (DirectedEdge directedEdge : outEdges) {
                if (messageReady(directedEdge)) {
                    if (msgOld == null) {
                        reuseNDList.add(oldFeature.getValue());
                        msgOld = MESSAGE(reuseNDList, false);
                        reuseNDList.clear();
                        reuseNDList.add(newFeature.getValue());
                        msgNew = MESSAGE(reuseNDList, false);
                        reuseNDList.clear();
                    }
                    reuseReduceMap.computeIfAbsent(directedEdge.getDest().getMasterPart(), item -> new ArrayList<>());
                    reuseReduceMap.get(directedEdge.getDest().getMasterPart()).add(directedEdge.getDest().getId());
                }
            }

            if (reuseReduceMap.isEmpty()) return;
            for (Map.Entry<Short, List<String>> shortTuple2Entry : reuseReduceMap.entrySet()) {
                Rmi.buildAndRun(
                        getId(),
                        getType(),
                        "receiveReplaceOutEdges",
                        shortTuple2Entry.getKey(),
                        OutputTags.ITERATE_OUTPUT_TAG,
                        shortTuple2Entry.getValue(),
                        msgNew,
                        msgOld
                );
            }
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
            for (String vertex : vertices) {
                Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(Tuple3.of(ElementType.VERTEX, vertex, "agg")), "replace", messageNew, messageOld);
            }
        }
    }
}
