package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.DirectedEdge;
import elements.Rmi;
import elements.Vertex;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.features.Tensor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.graph.OutputTags;

import java.util.*;

public class PartOptimizedStreamingGNNEmbedding extends StreamingGNNEmbedding {

    public PartOptimizedStreamingGNNEmbedding(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, trainableVertexEmbeddings);
    }

    /**
     * {@inheritDoc}
     */
    public void reduceOutEdges(Vertex v) {
        Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.OUT);
        final NDList[] msg = new NDList[1];
        HashMap<Short, List<String>> reduceMessages = null;
        for (DirectedEdge directedEdge : outEdges) {
            if (messageReady(directedEdge)) {
                if (Objects.isNull(msg[0])) {
                    msg[0] = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                    reduceMessages = new HashMap<>();
                }
                reduceMessages.computeIfAbsent(directedEdge.getDest().getMasterPart(), item -> new ArrayList<>());
                reduceMessages.get(directedEdge.getDest().getMasterPart()).add(directedEdge.getDest().getId());
            }
        }
        if (reduceMessages == null) return;
        for (Map.Entry<Short, List<String>> shortTuple2Entry : reduceMessages.entrySet()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "receiveReduceOutEdges",
                    shortTuple2Entry.getKey(),
                    OutputTags.ITERATE_OUTPUT_TAG,
                    shortTuple2Entry.getValue(),
                    msg[0]
            );
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        for (String vertex : vertices) {
            Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(Tuple3.of(ElementType.VERTEX, vertex, "agg")), "reduce", message, 1);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<DirectedEdge> outEdges = getRuntimeContext().getStorage().getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDList[] msgOld = new NDList[1];
        NDList[] msgNew = new NDList[1];
        HashMap<Short, List<String>> replaceMessages = null;
        for (DirectedEdge directedEdge : outEdges) {
            if (messageReady(directedEdge)) {
                if (Objects.isNull(msgOld[0])) {
                    msgOld[0] = MESSAGE(new NDList(oldFeature.getValue()), false);
                    msgNew[0] = MESSAGE(new NDList(newFeature.getValue()), false);
                    replaceMessages = new HashMap<>();
                }
                replaceMessages.computeIfAbsent(directedEdge.getDest().getMasterPart(), item -> new ArrayList<>());
                replaceMessages.get(directedEdge.getDest().getMasterPart()).add(directedEdge.getDest().getId());
            }
        }

        if (replaceMessages == null) return;
        for (Map.Entry<Short, List<String>> shortTuple2Entry : replaceMessages.entrySet()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "receiveReplaceOutEdges",
                    shortTuple2Entry.getKey(),
                    OutputTags.ITERATE_OUTPUT_TAG,
                    shortTuple2Entry.getValue(),
                    msgNew[0],
                    msgOld[0]
            );
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        for (String vertex : vertices) {
            Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(Tuple3.of(ElementType.VERTEX, vertex, "agg")), "replace", messageNew, messageOld);
        }
    }

}
