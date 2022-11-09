package elements;

import org.apache.flink.api.java.tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import storage.BaseStorage;

import javax.annotation.Nullable;
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
    @OmitStorage
    public List<Vertex> vertices;

    @OmitStorage
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

    public HEdge(String id, List<String> vertexIds, boolean halo, short master) {
        super(halo, master);
        this.vertexIds = vertexIds;
        this.id = id;
    }

    public HEdge(HEdge element, boolean deepCopy) {
        super(element, deepCopy);
        this.vertexIds = new ArrayList<>(element.vertexIds);
        this.id = element.id;
    }

    @Override
    public HEdge copy() {
        return new HEdge(this, false);
    }

    @Override
    public HEdge deepCopy() {
        return new HEdge(this, true);
    }

    /**
     * {@inheritDoc}
     * <strong>
     * Create vertices if not exist
     * </strong>
     */
    @Override
    public Consumer<Plugin> createElement() {
        // assert storage != null;
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                if (!storage.containsVertex(vertex.getId())) vertex.create();
            }
        }
        return super.createElement();
    }

    /**
     * {@inheritDoc}
     * <strong> Added partial vertex addition </strong>
     * <p>
     *     Memento element will contain [current_ids, ...newIds]
     * </p>
     */
    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, @Nullable GraphElement memento) {
        HEdge newHEdge = (HEdge) newElement;
        // assert storage != null;
        if (storage.layerFunction.getWrapperContext().getElement().getValue().getOp() == Op.COMMIT) {
            // This is external update for sure
            Set<String> t = HELPER_SET.get();
            t.clear();
            t.addAll(vertexIds);
            for (String vertexId : newHEdge.vertexIds) {
                if(t.contains(vertexId)) continue;
                if(memento == null) memento = copy();
                vertexIds.add(vertexId);
            }
        }
        return super.updateElement(newElement, memento);
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
    public String getVertexId(int pos){
        return vertexIds.get(pos);
    }

    /**
     * Get Vertices list or if empty try to retrieve from storage
     */
    public List<Vertex> getVertices() {
        if (vertices == null) {
            if (storage != null) {
                vertices = vertexIds.stream().map(item -> storage.getVertex(item)).collect(Collectors.toList());
                return vertices;
            }
            return Collections.emptyList();
        }
        else if(vertices.size() != vertexIds.size() && storage != null){
            for (int i = vertices.size(); i < vertexIds.size(); i++) {
                vertices.add(storage.getVertex(vertexIds.get(i)));
            }
        }
        return vertices;
    }

    /**
     * Get Vertex corresponding to given position
     */
    public Vertex getVertex(int pos){
        return getVertices().get(pos);
    }

    /**
     * {@inheritDoc}
     * <strong> Syncing only if some features have been updated </strong>
     */
    @Override
    public void update(GraphElement newElement) {
        assert state() == ReplicaState.MASTER || newElement.features == null;
        Tuple2<Consumer<Plugin>, GraphElement> tmp = updateElement(newElement, null);
        if (state() == ReplicaState.MASTER && tmp.f0 != null && !isHalo() && tmp.f1.features != null)
            syncReplicas(replicaParts()); // Only sync if the newElement brought in some new features otherwise need vertex addition should be local
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
        super.resume();
        if (vertices != null) {
            vertices.forEach(Vertex::delay);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        if (vertices != null) {
            vertices.forEach(item -> item.setStorage(storage));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearFeatures() {
        super.clearFeatures();
        if (vertices != null) {
            vertices.forEach(Vertex::clearFeatures);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType elementType() {
        return ElementType.HYPEREDGE;
    }
}
