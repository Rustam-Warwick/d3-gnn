package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.DEdge;
import elements.Rmi;
import elements.Vertex;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.MessageDirection;
import features.Tensor;

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
        Iterable<DEdge> outEdges = storage.getIncidentEdges(v, EdgeType.OUT);
        final NDList[] msg = new NDList[1];
        HashMap<Short, List<String>> reduceMessages = null;
        for (DEdge dEdge : outEdges) {
            if (this.messageReady(dEdge)) {
                if (Objects.isNull(msg[0])) {
                    msg[0] = MESSAGE(new NDList((NDArray) v.getFeature("f").getValue()), false);
                    reduceMessages = new HashMap<>();
                }
                reduceMessages.computeIfAbsent(dEdge.getDest().masterPart(), item -> new ArrayList<>());
                reduceMessages.get(dEdge.getDest().masterPart()).add(dEdge.getDest().getId());
            }
        }
        if (reduceMessages == null) return;
        for (Map.Entry<Short, List<String>> shortTuple2Entry : reduceMessages.entrySet()) {
            Rmi.buildAndRun(
                    new Rmi(getId(), "receiveReduceOutEdges", elementType(), new Object[]{shortTuple2Entry.getValue(), msg[0]}, false), storage,
                    shortTuple2Entry.getKey(),
                    MessageDirection.ITERATE
            );
        }
    }

    @RemoteFunction
    public void receiveReduceOutEdges(List<String> vertices, NDList message) {
        Rmi rmi = new Rmi(null, "reduce", null, new Object[]{message, 1}, true);
        for (String vertex : vertices) {
            Rmi.execute(storage.getAttachedFeature(ElementType.VERTEX, vertex, "agg", null), rmi);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<DEdge> outEdges = storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDList[] msgOld = new NDList[1];
        NDList[] msgNew = new NDList[1];
        HashMap<Short, List<String>> replaceMessages = null;
        for (DEdge dEdge : outEdges) {
            if (messageReady(dEdge)) {
                if (Objects.isNull(msgOld[0])) {
                    msgOld[0] = MESSAGE(new NDList(oldFeature.getValue()), false);
                    msgNew[0] = MESSAGE(new NDList(newFeature.getValue()), false);
                    replaceMessages = new HashMap<>();
                }
                replaceMessages.computeIfAbsent(dEdge.getDest().masterPart(), item -> new ArrayList<>());
                replaceMessages.get(dEdge.getDest().masterPart()).add(dEdge.getDest().getId());
            }
        }

        if (replaceMessages == null) return;
        for (Map.Entry<Short, List<String>> shortTuple2Entry : replaceMessages.entrySet()) {
            Rmi.buildAndRun(
                    new Rmi(getId(), "receiveReplaceOutEdges", elementType(), new Object[]{shortTuple2Entry.getValue(), msgNew[0], msgOld[0]}, false), storage,
                    shortTuple2Entry.getKey(),
                    MessageDirection.ITERATE
            );
        }
    }

    @RemoteFunction
    public void receiveReplaceOutEdges(List<String> vertices, NDList messageNew, NDList messageOld) {
        Rmi rmi = new Rmi(null, "replace", null, new Object[]{messageNew, messageOld}, true);
        for (String vertex : vertices) {
            Rmi.execute(storage.getAttachedFeature(ElementType.VERTEX, vertex, "agg", null), rmi);
        }
    }

}
