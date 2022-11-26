package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.DirectedEdge;
import elements.Rmi;
import elements.Vertex;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import elements.features.Tensor;

import java.util.*;

public class PartOptimizedStreamingGNNEmbeddingLayer extends StreamingGNNEmbeddingLayer {

    public PartOptimizedStreamingGNNEmbeddingLayer(String modelName) {
        super(modelName);
    }

    public PartOptimizedStreamingGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, trainableVertexEmbeddings);
    }

    public PartOptimizedStreamingGNNEmbeddingLayer(String modelName, boolean trainableVertexEmbeddings, boolean IS_ACTIVE) {
        super(modelName, trainableVertexEmbeddings, IS_ACTIVE);
    }


    /**
     * {@inheritDoc}
     */
    public void reduceOutEdges(Vertex v) {
        Iterable<DirectedEdge> outEdges = getStorage().getIncidentEdges(v, EdgeType.OUT);
        final NDList[] msg = new NDList[1];
        HashMap<Short, List<String>> reduceMessages = null;
        for (DirectedEdge directedEdge : outEdges) {
            if (this.messageReady(directedEdge)) {
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
                    MessageDirection.ITERATE,
                    shortTuple2Entry.getValue(),
                    msg[0]
            );
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        for (String vertex : vertices) {
            Rmi.execute(getStorage().getAttachedFeature(ElementType.VERTEX, vertex, "agg", null), "reduce", message, 1);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<DirectedEdge> outEdges = getStorage().getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
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
                    MessageDirection.ITERATE,
                    shortTuple2Entry.getValue(),
                    msgNew[0],
                    msgOld[0]
            );
        }
    }

    @RemoteFunction(triggerUpdate = false)
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        for (String vertex : vertices) {
            Rmi.execute(getStorage().getAttachedFeature(ElementType.VERTEX, vertex, "agg", null), "replace", messageNew, messageOld);
        }
    }

}
