package elements;

import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * HyperGraph Represents a subgraph which comprises hyperedges and a list of vertices
 * During construction all hyperedges will get shared vertices by reference
 */
public class HGraph extends GraphElement {

    public List<Vertex> vertices;

    public List<HEdge> hEdges;

    public HGraph() {

    }

    public HGraph(List<Vertex> vertices, List<HEdge> hEdges) {
        super();
        HashSet<Vertex> collect = new HashSet<>(vertices);
        for (HEdge hEdge : hEdges) {
            collect.addAll(hEdge.getVertices());
        }
        this.hEdges = hEdges;
        this.vertices = new ArrayList<>(collect);
    }

    @Override
    public GraphElement copy() {
        throw new NotImplementedException("Copy not implemented for HGraph");
    }

    @Override
    public GraphElement deepCopy() {
        throw new NotImplementedException("DeepCopy not implemented for HGraph");
    }

    public List<HEdge> gethEdges() {
        return hEdges;
    }

    public List<Vertex> getVertices() {
        return vertices;
    }

    /**
     * {@inheritDoc}
     * Create all the vertices and hyperedges
     */
    @Override
    public void create() {
        assert storage != null;
        for (Vertex vertex : vertices) {
            if (storage.containsVertex(vertex.getId())) storage.getVertex(vertex.getId()).update(vertex);
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

    // Override methods
    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        vertices.forEach(item -> item.setStorage(storage));
        hEdges.forEach(item -> item.setStorage(storage));
    }

    @Override
    public void resume() {
        super.resume();
        vertices.forEach(Vertex::resume);
        hEdges.forEach(HEdge::resume);
    }

    @Override
    public void delay() {
        super.delay();
        vertices.forEach(Vertex::delay);
        hEdges.forEach(HEdge::delay);
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        vertices.forEach(Vertex::clearFeatures);
        hEdges.forEach(HEdge::clearFeatures);
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
