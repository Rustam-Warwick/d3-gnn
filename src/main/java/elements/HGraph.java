package elements;

import storage.BaseStorage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


/**
 * HyperGraph Represents a subgraph which comprises hyperedges and a list of vertices
 * During construction all hyperedges will get shared vertices by reference
 */
public class HGraph extends GraphElement {

    public Vertex[] vertices;
    public HEdge[] hEdges;

    public HGraph() {

    }

    public HGraph(Vertex[] vertices, HEdge[] hEdges) {
        super();
        HashSet<Vertex> collect = new HashSet<>(List.of(vertices));
        for (HEdge hEdge : hEdges) {
            if (hEdge.getVertices() == null) continue;
            collect.addAll(Arrays.asList(hEdge.getVertices()));
            hEdge.vertices = null;
        }
        this.hEdges = hEdges;
        this.vertices = collect.toArray(Vertex[]::new);
    }

    @Override
    public GraphElement copy() {
        return null;
    }

    @Override
    public GraphElement deepCopy() {
        return null;
    }

    public HEdge[] getHyperEdges() {
        return hEdges;
    }

    public Vertex[] getVertices() {
        return vertices;
    }

    @Override
    public void create() {
        assert storage != null;
        for (Vertex vertex : vertices) {
            if (!storage.containsVertex(vertex.getId())) storage.getVertex(vertex.getId()).update(vertex);
            else vertex.create();
        }
        for (HEdge hEdge : hEdges) {
            if (storage.containsHyperEdge(hEdge.getId())) storage.getHyperEdge(hEdge.getId()).update(hEdge);
            else hEdge.create();
        }
    }

    @Override
    public void update(GraphElement newElement) {
        throw new IllegalStateException("SubGraph only support additions");
    }

    @Override
    public void delete() {
        throw new IllegalStateException("SubGraph only support additions");
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        for (Vertex vertex : vertices) {
            vertex.setStorage(storage);
        }
        for (HEdge hEdge : hEdges) {
            hEdge.setStorage(storage);
        }
    }

    @Override
    public void resume() {
        super.resume();
        for (Vertex vertex : vertices) {
            vertex.resume();
        }
        for (HEdge hEdge : hEdges) {
            hEdge.resume();
        }
    }

    @Override
    public void delay() {
        super.delay();
        for (Vertex vertex : vertices) {
            vertex.delay();
        }
        for (HEdge hEdge : hEdges) {
            hEdge.delay();
        }
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        for (Vertex vertex : vertices) {
            vertex.clearFeatures();
        }
        for (HEdge hEdge : hEdges) {
            hEdge.clearFeatures();
        }
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.GRAPH;
    }
}
