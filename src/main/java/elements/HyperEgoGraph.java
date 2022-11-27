package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.stream.Collectors;


/**
 * HyperGraph Represents a subgraph which comprises list of {@link HyperEdge} and a central {@link Vertex}
 *
 * @implNote A {@link HyperEgoGraph} will attempt to create {@link Vertex} but not update it, since latter can only happen in MASTER parts
 * @implNote GRAPH {@link ElementType}s are not meant for storage only used as a wrapper around a set of {@link GraphElement}
 */
public final class HyperEgoGraph extends GraphElement {

    public Vertex centralVertex;

    public List<HyperEdge> hyperEdges;

    public HyperEgoGraph() {
        super();
    }

    public HyperEgoGraph(Vertex centralVertex, List<String> hyperEdgeIds) {
        super();
        this.centralVertex = centralVertex;
        List<String> vertexIdList = List.of(centralVertex.getId());
        this.hyperEdges = hyperEdgeIds.stream().map(id -> new HyperEdge(id, vertexIdList)).collect(Collectors.toList());
    }

    /**
     * Throw {@link NotImplementedException}
     */
    @Override
    public GraphElement copy(CopyContext context) {
        throw new NotImplementedException("Copy not implemented for HGraph");
    }

    /**
     * Get the {@link HyperEdge}s stored
     */
    public List<HyperEdge> getHyperEdges() {
        return hyperEdges;
    }

    /**
     * Get the central {@link Vertex} stored
     */
    public Vertex getCentralVertex() {
        return centralVertex;
    }

    /**
     * {@inheritDoc}
     * Create all the vertices and hyperedges
     */
    @Override
    public void create() {
        if (!getStorage().containsVertex(centralVertex.getId())) centralVertex.create();
        for (HyperEdge hyperEdge : hyperEdges) {
            if (getStorage().containsHyperEdge(hyperEdge.getId()))
                getStorage().getHyperEdge(hyperEdge.getId()).update(hyperEdge);
            else hyperEdge.create();
        }
    }

    /**
     * throw {@link NotImplementedException}
     */
    @Override
    public void update(GraphElement newElement) {
        throw new NotImplementedException("SubGraph only support additions");
    }

    /**
     * throw {@link NotImplementedException}
     */
    @Override
    public void delete() {
        throw new NotImplementedException("SubGraph only support additions");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        centralVertex.resume();
        hyperEdges.forEach(HyperEdge::resume);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        centralVertex.resume();
        hyperEdges.forEach(HyperEdge::delay);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeserialized() {
        super.onDeserialized();
        centralVertex.onDeserialized();
        hyperEdges.forEach(HyperEdge::onDeserialized);
    }

    /**
     * Since EgoHyperGraphs are never stored it does not have an ID
     */
    @Override
    public String getId() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.GRAPH;
    }
}
