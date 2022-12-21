package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a Hyper-Edge in the Graph
 * A hyperEdge should have a unique ID similar to vertices
 *
 * @implNote A {@link HyperEdge} will attempt to create {@link Vertex} but not update it, since latter can only happen in MASTER parts
 * <p>
 * {@link HyperEdge} can be distributed where each part is going to store a partial subset of its vertices.
 * {@link HyperEdge} supports partial vertex additions by merging the current vertex set with the incoming set, hence it can also perform updates on REPLICAS
 * <strong>All {@link Vertex} IDs of incoming {@link HyperEdge} will be added together without duplication checks. Make sure you send them once </strong>
 * <strong>
 * In updates memento of {@link HyperEdge} will store the delta of vertex ids, in other words the new vertex ids that have been added
 * While, <code>this</code> will store previous vertex ids merged with the incoming changes in vertex ids
 * </strong>
 * </p>
 */
public final class HyperEdge extends ReplicableGraphElement {

    @Nullable
    public List<Vertex> vertices;

    public List<String> vertexIds;

    public String id;

    public HyperEdge() {
        super();
    }

    public HyperEdge(String id, List<String> vertexIds) {
        super();
        this.id = id;
        this.vertexIds = vertexIds;
    }

    public HyperEdge(String id, List<String> vertexIds, List<Vertex> vertices) {
        super();
        this.id = id;
        this.vertexIds = vertexIds;
        this.vertices = vertices;
    }

    public HyperEdge(String id, List<String> vertexIds, List<Vertex> vertices, short master) {
        super(master);
        this.id = id;
        this.vertexIds = vertexIds;
        this.vertices = vertices;
    }

    public HyperEdge(String id, List<String> vertexIds, short master) {
        super(master);
        this.vertexIds = vertexIds;
        this.id = id;
    }

    public HyperEdge(HyperEdge element, CopyContext context) {
        super(element, context);
        id = element.id;
        if (context == CopyContext.SYNC) {
            vertexIds = Collections.emptyList();
        } else {
            vertexIds = element.vertexIds;
            vertices = element.vertices;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HyperEdge copy(CopyContext context) {
        return new HyperEdge(this, context);
    }

    /**
     * {@inheritDoc}
     * <strong>
     * Also create vertices if not exist similar to {@link DirectedEdge}
     * </strong>
     */
    @Override
    public void createInternal() {
        for (int i = 0; i < vertexIds.size(); i++) {
            if (!getGraphRuntimeContext().getStorage().containsVertex(vertexIds.get(i))) vertices.get(i).create();
        }
        super.createInternal();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Different from normal Replication HEdge can have updates in replicas if it is not bringing a new Feature because of partial vertex addition logic
     * </p>
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) super.update(newElement);
        else if (state() == ReplicaState.REPLICA) {
            assert newElement.features == null;
            updateInternal(newElement);
        } else {
            throw new IllegalStateException("Not defined if this HyperEdge is Replica or Master");
        }
    }


    /**
     * {@inheritDoc}
     * <strong> Added partial vertex addition logic </strong>
     *
     * @implNote Memento of {@link HyperEdge} stores the newly added {@link Vertex} ids!!!
     */
    @Override
    public void updateInternal(GraphElement newElement) {
        HyperEdge newHyperEdge = (HyperEdge) newElement;
        for (int i = 0; i < newHyperEdge.vertexIds.size(); i++) {
            if (!getGraphRuntimeContext().getStorage().containsVertex(newHyperEdge.vertexIds.get(i)))
                newHyperEdge.vertices.get(i).create();
        }
        vertexIds.addAll(newHyperEdge.vertexIds);
        super.updateInternal(newElement);
    }

    /**
     * Get Vertices id list
     */
    public List<String> getVertexIds() {
        return vertexIds;
    }

    /**
     * Get vertex id at the given position
     */
    public String getVertexId(int pos) {
        return vertexIds.get(pos);
    }

    /**
     * Get Vertices list or if empty try to retrieve from storage
     */
    public List<Vertex> getVertices() {
        if (vertices == null) {
            if (getGraphRuntimeContext() != null) {
                vertices = vertexIds.stream().map(item -> getGraphRuntimeContext().getStorage().getVertex(item)).collect(Collectors.toList());
                return vertices;
            }
            return Collections.emptyList();
        } else if (vertices.size() != vertexIds.size() && getGraphRuntimeContext() != null) {
            for (int i = vertices.size(); i < vertexIds.size(); i++) {
                vertices.add(getGraphRuntimeContext().getStorage().getVertex(vertexIds.get(i)));
            }
        }
        return vertices;
    }

    /**
     * Get Vertex corresponding to given position
     */
    public Vertex getVertex(int pos) {
        return getVertices().get(pos);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        if (vertices != null) {
            vertices.forEach(Vertex::resume);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        if (vertices != null) {
            vertices.forEach(Vertex::delay);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeserialized() {
        super.onDeserialized();
        if (vertices != null) {
            vertices.forEach(Vertex::onDeserialized);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "HEdge{" +
                "vertexIds=" + vertexIds +
                ", id='" + getId() + '\'' +
                ", master=" + getMasterPart() +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.HYPEREDGE;
    }
}
