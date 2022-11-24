package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.commons.lang3.NotImplementedException;
import storage.BaseStorage;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * HyperGraph Represents a subgraph which comprises list of {@link HyperEdge} and a central {@link Vertex}
 *
 * @implNote A {@link EgoHyperGraph} will attempt to create {@link Vertex} but not update it, since latter can only happen in MASTER parts
 * @implNote GRAPH {@link ElementType}s are not meant for storage only used as a wrapper around a set of {@link GraphElement}
 */
public class EgoHyperGraph extends GraphElement {

    public Vertex centralVertex;

    public List<HyperEdge> hyperEdges;

    public EgoHyperGraph() {
        super();
    }

    public EgoHyperGraph(Vertex centralVertex, List<String> hyperEdgeIds) {
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

    public List<HyperEdge> getHyperEdges() {
        return hyperEdges;
    }

    public Vertex getCentralVertex() {
        return centralVertex;
    }

    /**
     * {@inheritDoc}
     * Create all the vertices and hyperedges
     */
    @Override
    public Consumer<BaseStorage> create() {
        Consumer<BaseStorage> callback = null;
        if (!getStorage().containsVertex(centralVertex.getId())) callback = chain(callback, centralVertex.create());
        for (HyperEdge hyperEdge : hyperEdges) {
            if (getStorage().containsHyperEdge(hyperEdge.getId()))
                callback = chain(callback, getStorage().getHyperEdge(hyperEdge.getId()).update(hyperEdge));
            else callback = chain(callback, hyperEdge.create());
        }
        return callback;
    }

    /**
     * throw {@link NotImplementedException}
     */
    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        throw new NotImplementedException("SubGraph only support additions");
    }

    /**
     * throw {@link NotImplementedException}
     */
    @Override
    public Consumer<BaseStorage> delete() {
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
