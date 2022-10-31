package elements;

import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a Hyper-Edge in the Graph
 * A hyperEdge should have a unique Id similar to Vertices
 */
public class HEdge extends ReplicableGraphElement {

    @Nullable
    @OmitStorage
    public Vertex[] vertices;
    @OmitStorage
    public HashSet<String> vertexIds; // This we need to store since the naming convention of HEdge does not imply the vertex ids

    public String id;

    public HEdge() {

    }

    public HEdge(String id, @Nullable Vertex[] vertices) {
        super();
        this.id = id;
        vertexIds = new HashSet<>();
        this.vertices = vertices;
        if (vertices != null) vertexIds.addAll(Arrays.stream(vertices).map(Vertex::getId).collect(Collectors.toList()));
    }

    public HEdge(HEdge element, boolean deepCopy) {
        super(element, deepCopy);
        this.vertices = null;
        this.vertexIds = new HashSet<>(element.vertexIds); // Create new reference to comprase additions
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
     * HyperEdge on create should arrive with vertices otherwise a zero-vertex is assumed
     */
    @Override
    public Consumer<Plugin> createElement() {
        vertexIds.clear();
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                if (!storage.containsVertex(vertex.getId())) vertex.create();
                vertexIds.add(vertex.getId());
            }
        }
        vertices = null; // Set to null to then access from the storage, similar to edge
        return super.createElement();
    }

    /**
     * Append only set of vertex ids in the hyperedge
     */
    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, @Nullable GraphElement memento) {
        HEdge newHEdge = (HEdge) newElement;
        if (newHEdge.vertices != null) {
            // This is not possible during SYNC phases so we are safe from merging vertexIds across replicas
            // The reason why this is not possible is because SYNC is copying this element which drops the vertices
            // If there is an addition of new vertices to this hyperedge we should update it even if features of it are not updated
            for (Vertex vertex : newHEdge.vertices) {
                if (!vertexIds.contains(vertex.getId())) {
                    if (memento == null) memento = copy();
                    vertexIds.add(vertex.getId());
                    if (!storage.containsVertex(vertex.getId())) vertex.create();
                }
            }
        }
        return super.updateElement(newElement, memento);
    }

    /**
     * master -> update element, if changed send message to replica
     * replica -> Redirect to master, false message
     *
     * @param newElement newElement to update with
     */
    @Override
    public void update(GraphElement newElement) {
        if (state() == ReplicaState.MASTER) {
            Tuple2<Consumer<Plugin>, GraphElement> tmp = updateElement(newElement, null);
            if (tmp.f0 != null && !isHalo() && newElement.features != null)
                syncReplicas(replicaParts()); // Only sync if the newElement brought in some new features
        } else throw new IllegalStateException("Replicable element but don't know if master or repica");
    }

    /**
     * Get all the Vertices of the given HyperEdge
     */
    @Nullable
    public Vertex[] getVertices() {
        if ((vertices == null || vertices.length != vertexIds.size()) && storage != null) {
            // If there is a mismatch between vertexIds and vertices re-compute the array
            vertices = new Vertex[vertexIds.size()];
            int i = 0;
            for (String vertexId : vertexIds) {
                vertices[i++] = storage.getVertex(vertexId);
            }
        }
        return vertices;
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                vertex.setStorage(storage);
            }
        }
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                vertex.clearFeatures();
            }
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ElementType elementType() {
        return ElementType.HYPEREDGE;
    }
}
