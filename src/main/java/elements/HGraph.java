package elements;

import ai.djl.ndarray.NDArray;
import storage.BaseStorage;

import java.util.HashMap;
import java.util.function.Consumer;


/**
 * HyperGraph Represents a subgraph which comprises hyperedges and a list of vertices
 * During construction all hyperedges will get shared vertices by reference
 */
public class HGraph extends GraphElement{
    public Vertex[] vertices;
    public HEdge[] hEdges;

    public HGraph() {

    }

    public HGraph(Vertex[] vertices, HEdge[] hEdges){
        super("graph");
        HashMap<String, Vertex> setVertices = new HashMap<>();
        for (Vertex vertex : vertices) {
            setVertices.putIfAbsent(vertex.getId(), vertex);
        }
        for (HEdge hEdge : hEdges) {
            for (int i = 0; i < hEdge.getVertices().length; i++) {
                setVertices.putIfAbsent(hEdge.getVertices()[i].getId(), hEdge.getVertices()[i]);
                hEdge.getVertices()[i] = setVertices.get(hEdge.getVertices()[i].getId());
            }
        }
        this.hEdges = hEdges;
        this.vertices = setVertices.values().toArray(Vertex[]::new);
    }

    public HEdge[] getHyperEdges() {
        return hEdges;
    }

    public Vertex[] getVertices() {
        return vertices;
    }

    @Override
    public Consumer<Plugin> createElement() {
        assert storage!=null;
        if(vertices != null){
            for (Vertex vertex : vertices) {
                if(storage.containsVertex(vertex.getId()))storage.getVertex(vertex.getId()).update(vertex);
                else vertex.create();
            }
        }
        if(hEdges != null){
            for (HEdge hEdge : hEdges) {
                if(storage.containsHyperEdge(hEdge.getId())) storage.getHyperEdge(hEdge.getId()).update(hEdge);
                else hEdge.create();
            }
        }
        return null;
    }

    @Override
    public void update(GraphElement newElement) {
        throw new IllegalStateException("Update not available for subgraph grannularity");
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        if(vertices != null){
            for (Vertex vertex : vertices) {
                vertex.setStorage(storage);
            }
        }
        if(hEdges != null){
            for (HEdge hEdge : hEdges) {
                hEdge.setStorage(storage);
            }
        }
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        for (Vertex vertex : vertices) {
            vertex.applyForNDArrays(operation);
        }
        for (HEdge hEdge : hEdges) {
            hEdge.applyForNDArrays(operation);
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.GRAPH;
    }
}
