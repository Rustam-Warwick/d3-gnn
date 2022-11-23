package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.commons.lang3.NotImplementedException;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;


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
    public GraphElement copy(CopyContext context) {
        throw new NotImplementedException("Copy not implemented for HGraph");
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
     *
     * @return
     */
    @Override
    public Consumer<BaseStorage> create() {
        for (Vertex vertex : vertices) {
            if (getStorage().containsVertex(vertex.getId())) getStorage().getVertex(vertex.getId()).update(vertex);
            else vertex.create();
        }
        for (HEdge hEdge : hEdges) {
            if (getStorage().containsHyperEdge(hEdge.getId())) getStorage().getHyperEdge(hEdge.getId()).update(hEdge);
            else hEdge.create();
        }
        return null;
    }

    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        throw new IllegalStateException("SubGraph only support additions");
    }

    @Override
    public Consumer<BaseStorage> delete() {
        throw new IllegalStateException("SubGraph only support additions");
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
    public String getId() {
        return null;
    }

    @Override
    public ElementType getType() {
        return ElementType.GRAPH;
    }
}
