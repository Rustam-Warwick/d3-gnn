package storage;
import edge.BaseEdge;
import features.Feature;
import part.BasePart;
import vertex.BaseVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * HashMap Based Vertex-Centric graph storage
 */
public class HashMapGraphStorage<VT extends BaseVertex> extends GraphStorage<VT> {
    /**
     * Stores Edges as a map of (source_key=>(dest_key))
     */
    public HashMap<String, ArrayList<BaseEdge<VT>>> edges;
    /**
     * Stores Vertex hashed by source id. Good for O(1) search
     * Note that dest vertices are stored here as well with the isPart attribute set to something else
     */
    public HashMap<String, VT> vertices;

    public HashMapGraphStorage(BasePart<VT> part){
        super(part);
        edges = new HashMap<>();
        vertices = new HashMap<>();

    }

    public HashMapGraphStorage() {
        super();
        edges = new HashMap<>();
        vertices = new HashMap<>();
    }

    @Override
    public VT addVertex(VT v) {
        // If vertex is already here then discard it
        if(vertices.containsKey(v.getId()))return null;
        VT vC = (VT) v.copy();
        vC.setStorage(this);
        vC.addVertexCallback();
        vertices.put(v.getId(), vC);
        return vC;
    }

    @Override
    public boolean deleteVertex(VT v) {
        return false;
    }

//    @Override
//    @Deprecated
//    public void updateVertex(Feature f) {
//        if(!vertices.containsKey(f.attachedId))return;
//        getVertex(f.attachedId).updateFeatureCallback(c,f);
//    }

    @Override
    public BaseEdge<VT> addEdge(BaseEdge<VT> e) {
        // 1. If source vertex not in storage create it
        this.addVertex(e.source);
        this.addVertex(e.destination);
        // 2. Create Edge
        edges.putIfAbsent(e.source.getId(),new ArrayList<>());
        BaseEdge<VT> eC = e.copy();
        eC.source = this.getVertex(e.source.getId());
        eC.destination = this.getVertex(e.destination.getId());
        eC.setStorage(this);
        edges.get(e.source.getId()).add(eC);
        // 3. Make Edge Callback & Return
        eC.addEdgeCallback();
        return eC;
    }

    @Override
    public void deleteEdge(BaseEdge<VT> e) {

    }

    @Override
    public void updateFeature(Feature.Update<?> e) {
        try{
            Class<?> featureClass = Class.forName(e.attachedToClassName);
            if(BaseVertex.class.isAssignableFrom(featureClass)){
                // Vertex feature
                VT vertex = this.getVertex(e.attachedId);
                if(vertex==null)return;
                vertex.updateFeature(e);
            }
            else if(BaseEdge.class.isAssignableFrom(featureClass)){
                // Edge feature
            }
        }catch(ClassNotFoundException ce){
            System.out.println(ce.getMessage());
        }catch (NullPointerException ne){
            System.out.println(e.fieldName);
        }
    }

    // Get Queries
    @Override
    public VT getVertex(String id) {
        return this.vertices.get(id);
    }

    @Override
    public BaseEdge<VT> getEdge() {
        return null;
    }


    @Override
    public Stream<VT> getVertices() {
        return vertices.values().stream();
    }

    @Override
    public Stream<BaseEdge<VT>> getEdges() {
        return edges.values().stream().flatMap(Collection::stream);
    }
}
