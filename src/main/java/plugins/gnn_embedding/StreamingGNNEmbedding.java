package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import elements.annotations.RemoteFunction;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import elements.features.Tensor;
import functions.metrics.MovingAverageCounter;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import storage.ObjectPoolScope;

import java.util.List;

/**
 * Plugin that compute the streaming GNN for source based messages
 */
public class StreamingGNNEmbedding extends BaseGNNEmbedding {

    protected transient Counter throughput;

    protected transient Counter latency;

    protected transient NDList reuseFeaturesNDList;

    protected transient Tuple3<ElementType, Object, String> reuseAggId;

    protected transient Short2ObjectOpenHashMap<ObjectArrayList<String>> reusePart2VertexIdsMap;

    protected transient Tensor reuseTensor;

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
        reuseFeaturesNDList = new NDList();
        reuseAggId = Tuple3.of(ElementType.VERTEX, null, "agg");
        reusePart2VertexIdsMap = new Short2ObjectOpenHashMap<>();
        reuseTensor = new Tensor("f", null, false);
        reuseTensor.id.f0 = ElementType.VERTEX;
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
            if (directedEdge.getSrc().containsFeature("f")) {
                reuseFeaturesNDList.clear();
                reuseFeaturesNDList.add((NDArray) directedEdge.getSrc().getFeature("f").getValue());
                NDList msg = MESSAGE(reuseFeaturesNDList, false);
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
                reduceOutEdges((Tensor) feature, (Vertex) feature.getElement());
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
                updateOutEdges((Tensor) feature, (Tensor) oldFeature, (Vertex) feature.getElement());
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
        reuseFeaturesNDList.clear();
        reuseFeaturesNDList.add(ft);
        reuseFeaturesNDList.add(agg);
        NDArray update = UPDATE(reuseFeaturesNDList, false).get(0);
        reuseTensor.id.f1 = v.getId();
        reuseTensor.value = update;
        getRuntimeContext().output(new GraphOp(Op.COMMIT, v.getMasterPart(), reuseTensor));
        throughput.inc();
        latency.inc(getRuntimeContext().getTimerService().currentProcessingTime() - getRuntimeContext().currentTimestamp());
    }

    /**
     * Given vertex reduce all of its out edges
     * <p>
     * Now we are assuming that the vertex function only depends on the source "f"
     * Hence we can optimize by sending single tensor to the parts
     * </p>
     */
    public void reduceOutEdges(Tensor feature, Vertex v) {
        NDList message = null;
        reusePart2VertexIdsMap.forEach((key, val) -> val.clear());
        try (ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (DirectedEdge directedEdge : getRuntimeContext().getStorage().getEdges().filterSrcId(v.getId())) {
                if (message == null) {
                    reuseFeaturesNDList.clear();
                    reuseFeaturesNDList.add(feature.getValue());
                    message = MESSAGE(reuseFeaturesNDList, false);
                }
                reusePart2VertexIdsMap.computeIfAbsent(directedEdge.getDest().getMasterPart(), (k) -> new ObjectArrayList<>()).add(directedEdge.getDestId());
                objectPoolScope.refresh();
            }
        }
        final NDList finalMessage = message;
        reusePart2VertexIdsMap.forEach((part, values) -> {
            if (values.isEmpty()) return;
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "receiveReduceOutEdges",
                    part,
                    OutputTags.ITERATE_OUTPUT_TAG,
                    values,
                    finalMessage
            );
        });
    }

    /**
     * Receive the reduction messages and commit
     */
    @RemoteFunction(triggerUpdate = false)
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        try (ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (String vertexId : vertices) {
                Rmi.execute(getRuntimeContext().getStorage().getVertices().getFeatures(vertexId).get("agg"), "reduce", message, 1);
                objectPoolScope.refresh();
            }
        }
    }

    /**
     * Given vertex replace all of its out edges
     * <p>
     * Now we are assuming that the vertex function only depends on the source "f"
     * Hence we can optimize by sending single tensor to the parts
     * </p>
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature, Vertex vertex) {
        NDList msgOld = null;
        NDList msgNew = null;
        reusePart2VertexIdsMap.forEach((key, val) -> val.clear());
        try (ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (DirectedEdge directedEdge : getRuntimeContext().getStorage().getEdges().filterSrcId(vertex.getId())) {
                if (msgOld == null) {
                    reuseFeaturesNDList.clear();
                    reuseFeaturesNDList.add(oldFeature.getValue());
                    msgOld = MESSAGE(reuseFeaturesNDList, false);
                    reuseFeaturesNDList.clear();
                    reuseFeaturesNDList.add(newFeature.getValue());
                    msgNew = MESSAGE(reuseFeaturesNDList, false);
                    reuseFeaturesNDList.clear();
                }
                reusePart2VertexIdsMap.computeIfAbsent(directedEdge.getDest().getMasterPart(), (k) -> new ObjectArrayList<>()).add(directedEdge.getDestId());
                objectPoolScope.refresh();
            }
        }
        final NDList finalMsgOld = msgOld;
        final NDList finalMsgNew = msgNew;
        reusePart2VertexIdsMap.forEach((part, values) -> {
            if (values.isEmpty()) return;
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "receiveReplaceOutEdges",
                    part,
                    OutputTags.ITERATE_OUTPUT_TAG,
                    values,
                    finalMsgNew,
                    finalMsgOld
            );
        });
    }

    /**
     * Receive the replacement messages and commit to memory
     */
    @RemoteFunction(triggerUpdate = false)
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        try (ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (String vertexId : vertices) {
                Rmi.execute(getRuntimeContext().getStorage().getVertices().getFeatures(vertexId).get("agg"), "replace", messageNew, messageOld);
                objectPoolScope.refresh();
            }
        }
    }
}
