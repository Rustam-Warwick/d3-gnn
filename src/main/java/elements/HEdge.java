package elements;

import elements.enums.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a Hyper-Edge in the Graph
 * A hyperEdge should have a unique Id similar to Vertices
 * <p>
 * Vertices and vertexIds should be 1-1 mapping
 * Constructor ensures this
 * Vertices might be null though
 * </p>
 */

public final class HEdge extends ReplicableGraphElement {

    private static final ThreadLocal<Set<String>> HELPER_SET = ThreadLocal.withInitial(HashSet::new);

    @Nullable
    public List<Vertex> vertices;

    public List<String> vertexIds;

    public String id;

    public HEdge() {

    }

    public HEdge(String id, @NotNull List<Vertex> vertices) {
        super();
        this.id = id;
        this.vertices = vertices;
        this.vertexIds = vertices.stream().map(Vertex::getId).collect(Collectors.toList());
    }

    public HEdge(String id, List<String> vertexIds, short master) {
        super(master);
        this.vertexIds = vertexIds;
        this.id = id;
    }

    public HEdge(HEdge element, CopyContext context) {
        super(element, context);
        id = element.id;
        if (context != CopyContext.SYNC) {
            vertexIds = element.vertexIds;
            vertices = element.vertices;
        }
    }

    @Override
    public HEdge copy(CopyContext context) {
        return new HEdge(this, context);
    }

    /**
     * {@inheritDoc}
     * <strong>
     * Create vertices if not exist
     * </strong>
     */
    @Override
    public Consumer<BaseStorage> createInternal() {
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                if (!getStorage().containsVertex(vertex.getId())) vertex.create();
            }
        }
        return super.createInternal();
    }

    /**
     * {@inheritDoc}
     * <strong> Added partial vertex addition </strong>
     * <p>
     * Memento element will contain [current_ids, ...newIds]
     * </p>
     */
    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> updateInternal(GraphElement newElement, @Nullable GraphElement memento) {
        HEdge newHEdge = (HEdge) newElement;
        if (getStorage().layerFunction.getWrapperContext().getElement().getValue().getOp() == Op.SYNC)
            return super.updateInternal(newElement, memento);
        Set<String> t = HELPER_SET.get();
        t.clear();
        t.addAll(vertexIds);
        for (String vertexId : newHEdge.vertexIds) {
            if (t.contains(vertexId)) continue;
            if (memento == null) {
                HEdge tmp = copy(CopyContext.MEMENTO); // Create new array to compute the difference
                tmp.vertexIds = new ArrayList<>(tmp.vertexIds);
                memento = tmp;
            }
            vertexIds.add(vertexId);
        }
        return super.updateInternal(newElement, memento);
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
     * <p>
     * Different from normal Replication HEdge can have updates in replicas if it is not bringing a new Feature
     * </p>
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> update(GraphElement newElement) {
        assert state() == ReplicaState.MASTER || newElement.features == null;
        Tuple2<Consumer<BaseStorage>, GraphElement> tmp = updateInternal(newElement, null);
        if (state() == ReplicaState.MASTER && tmp.f0 != null && newElement.features != null && !getReplicaParts().isEmpty()) {
            HEdge cpy = copy(CopyContext.SYNC); // Make a copy do not actually send this element
            getReplicaParts().forEach(part_id -> getStorage().layerFunction.message(new GraphOp(Op.SYNC, part_id, cpy), MessageDirection.ITERATE));
        }
        return tmp;
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
