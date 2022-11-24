package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a Hyper-Edge in the Graph
 * A hyperEdge should have a unique ID similar to vertices
 *
 * @implNote A {@link HyperEdge} will attempt to create {@link Vertex} but not update it, since latter can only happen in MASTER parts
 * <p>
 * {@link HyperEdge} can be distributed where each part is going to store a partial subset of its vertices.
 * {@link HyperEdge} supports partial vertex additions by merging the current vertex set with the incoming set, hence it can also perform updates on REPLICAS
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
        if (context != CopyContext.SYNC) {
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
    public Consumer<BaseStorage> createInternal() {
        Consumer<BaseStorage> callback = null;
        for (int i = 0; i < vertexIds.size(); i++) {
            if (!getStorage().containsVertex(vertexIds.get(i))) callback = chain(callback, vertices.get(i).create());
        }
        return chain(callback, super.createInternal());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Different from normal Replication HEdge can have updates in replicas if it is not bringing a new Feature because of partial vertex addition logic
     * </p>
     */
    @Override
    public Consumer<BaseStorage> update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) return super.update(newElement);
        else if (state() == ReplicaState.REPLICA) {
            assert newElement.features == null;
            return updateInternal(newElement);
        } else {
            throw new IllegalStateException("Not defined if this HyperEdge is Replica or Master");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * super.updateInternal skips the partial vertex update logic, since hyperedges should not replicated their partial vertex set
     * </p>
     */
    @Override
    public void sync(GraphElement newElement) {
        getStorage().runCallback(super.updateInternal(newElement));
    }

    /**
     * {@inheritDoc}
     * <strong> Added partial vertex addition </strong>
     */
    @Override
    public Consumer<BaseStorage> updateInternal(GraphElement newElement) {
        HyperEdge newHyperEdge = (HyperEdge) newElement;
        Consumer<BaseStorage> callback = null;
        for (int i = 0; i < newHyperEdge.vertexIds.size(); i++) {
            if (!getStorage().containsVertex(newHyperEdge.vertexIds.get(i)))
                callback = chain(callback, newHyperEdge.vertices.get(i).create());
        }
        return chain(callback, storage -> {
            List<String> currentVIdList = List.copyOf(vertexIds);
            vertexIds.addAll(newHyperEdge.vertexIds);
            newHyperEdge.vertexIds = currentVIdList;
        }).andThen(super.updateInternal(newElement));
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
            if (getStorage() != null) {
                vertices = vertexIds.stream().map(item -> getStorage().getVertex(item)).collect(Collectors.toList());
                return vertices;
            }
            return Collections.emptyList();
        } else if (vertices.size() != vertexIds.size() && getStorage() != null) {
            for (int i = vertices.size(); i < vertexIds.size(); i++) {
                vertices.add(getStorage().getVertex(vertexIds.get(i)));
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
