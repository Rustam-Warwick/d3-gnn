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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@inheritDoc}
 */
public class StreamingGNNEmbedding extends BaseGNNEmbedding {

    protected transient Counter throughput; // Throughput counter, only used for last layer

    protected transient Counter latency; // Throughput counter, only used for last layer

    protected transient NDList reuseNDList; // Reusable NDList object for performance

    protected transient Tuple3<ElementType, Object, String> reuseAggId; // Reusable Feature id tuple

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
        latency = new MovingAverageCounter(1000);
        getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        getRuntimeContext().getMetricGroup().counter("latency", latency);
        reuseNDList = new NDList(2);
        reuseAggId = Tuple3.of(ElementType.VERTEX, null, "agg");
        reuseReduceMap = new Short2ObjectOpenHashMap<>();
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
                reuseNDList.clear();
                reuseNDList.add((NDArray) directedEdge.getSrc().getFeature("f").getValue());
                NDList msg = MESSAGE(reuseNDList, false);
                reuseAggId.f1 = directedEdge.getDestId();
                Rmi.buildAndRun(
                        reuseAggId,
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
                reduceOutEdgesOptimized((Vertex) feature.getElement());
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
            if (feature.id.f0 == ElementType.VERTEX && "f".equals(feature.getName())) {
                updateOutEdgesOptimized((Tensor) feature, (Tensor) oldFeature);
                if (feature.state() == ReplicaState.MASTER) forward((Vertex) feature.getElement());
            }
            if (feature.id.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
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
        reuseNDList.clear();
        reuseNDList.add(ft);
        reuseNDList.add(agg);
        NDArray update = UPDATE(reuseNDList, false).get(0);
        Tensor tmp = new Tensor("f", update, false);
        tmp.id.f0 = ElementType.VERTEX;
        tmp.id.f1 = v.getId();
        getRuntimeContext().output(new GraphOp(Op.COMMIT, v.getMasterPart(), tmp));
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
    public void reduceOutEdgesOptimized(Vertex v) {
        Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.OUT);
        NDList result = null;
        reuseReduceMap.clear();
        for (DirectedEdge directedEdge : outEdges) {
            if (result == null) {
                reuseNDList.clear();
                reuseNDList.add((NDArray) v.getFeature("f").getValue());
                result = MESSAGE(reuseNDList, false);
            }
            reuseReduceMap.compute(directedEdge.getDest().getMasterPart(), (key, item) -> {
                if(item == null) item = new ArrayList<>();
                item.add(directedEdge.getDestId());
                return item;
            });
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

    @RemoteFunction(triggerUpdate = false)
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        for (String vertexId : vertices) {
            reuseAggId.f1 = vertexId;
            Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(reuseAggId), "reduce", message, 1);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOutEdgesOptimized(Tensor newFeature, Tensor oldFeature) {
        Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDList msgOld = null;
        NDList msgNew = null;
        reuseReduceMap.clear();
        for (DirectedEdge directedEdge : outEdges) {
            if (msgOld == null) {
                reuseNDList.clear();
                reuseNDList.add(oldFeature.getValue());
                msgOld = MESSAGE(reuseNDList, false);
                reuseNDList.clear();
                reuseNDList.add(newFeature.getValue());
                msgNew = MESSAGE(reuseNDList, false);
                reuseNDList.clear();
            }
            reuseReduceMap.compute(directedEdge.getDest().getMasterPart(), (key, item)->{
                if(item == null) item = new ArrayList<>();
                item.add(directedEdge.getDestId());
                return item;
            });
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

    @RemoteFunction(triggerUpdate = false)
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        for (String vertexId : vertices) {
            reuseAggId.f1 = vertexId;
            Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(reuseAggId), "replace", messageNew, messageOld);
        }
    }
}
