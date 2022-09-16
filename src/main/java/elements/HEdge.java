package elements;

import ai.djl.ndarray.NDArray;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.function.Consumer;

/**
 * This class represents a hyper-edge(Net) connecting more than 1 vertices
 */
public class HEdge extends ReplicableGraphElement {

    @Nullable
    @OmitStorage
    public Vertex[] vertices;

    public HashSet<String> vertexIds; // This we need to store since the naming convention of HEdge does not imply the vertex ids

    public HEdge() {

    }

    public HEdge(String id, @Nullable Vertex[] vertices) {
        this(id, false, null, vertices);
    }

    public HEdge(HEdge element, boolean deepCopy) {
        super(element, deepCopy);
        this.vertices = null;
        this.vertexIds = new HashSet<>(element.vertexIds); // Create new reference to comprase additions
    }

    public HEdge(String id, boolean halo, Short master, Vertex[] vertices) {
        super(id, halo, master);
        this.vertices = vertices;
        vertexIds = new HashSet<>();
        for (int i = 0; i < vertices.length; i++) {
            vertexIds.add(vertices[i].getId());
        }
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
    public Boolean createElement() {
        vertexIds.clear();
        if(vertices != null){
            for (int i = 0; i < vertices.length; i++) {
                if(!storage.containsVertex(vertices[i].getId())) vertices[i].create();
                vertexIds.add(vertices[i].getId());
            }
        }
        vertices = null; // Set to null to then access from the storage, similar to edge
        return super.createElement();
    }

    /**
     * Append only set of vertex ids in the hyperedge
     */
    @Override
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement, @Nullable GraphElement memento) {
        HEdge newHEdge = (HEdge) newElement;
        if(newHEdge.vertices != null){
            // This is not possible during SYNC phases so we are safe from merging vertexIds across replicas
            // The reason why this is not possible is because SYNC is copying this element which drops the vertices
            for (Vertex vertex : newHEdge.vertices) {
                if(!vertexIds.contains(vertex.getId())){
                    if(memento == null) memento = copy();
                    vertexIds.add(vertex.getId());
                    if(!storage.containsVertex(vertex.getId()))vertex.create();
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
            Tuple2<Boolean, GraphElement> tmp = updateElement(newElement, null);
            if(tmp.f0 && !isHalo() && tmp.f1.features != null) syncReplicas(replicaParts());
        } else throw new IllegalStateException("Replicable element but don't know if master or repica");
    }

    /**
     * Get all the Vertices of the given HyperEdge
     */
    @Nullable
    public Vertex[] getVertices() {
        if((vertices == null || vertices.length != vertexIds.size()) && storage != null){
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
        if(vertices != null){
            for (Vertex vertex : vertices) {
                vertex.setStorage(storage);
            }
        }
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        if(vertices != null){
            for (Vertex vertex : vertices) {
                vertex.clearFeatures();
            }
        }
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
//        if(vertices != null){
//            for (Vertex vertex : vertices) {
//                vertex.applyForNDArrays(operation);
//            }
//        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.HYPEREDGE;
    }
}
