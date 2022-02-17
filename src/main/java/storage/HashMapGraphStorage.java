package storage;
import edge.BaseEdge;
import features.Feature;
import javassist.NotFoundException;
import org.apache.commons.lang3.NotImplementedException;
import part.BasePart;
import vertex.BaseVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * HashMap Based Vertex-Centric graph storage
 */
public class HashMapGraphStorage extends GraphStorage {
    /**
     * Stores Edges as a map of (source_key=>(dest_key))
     */
    public HashMap<String, ArrayList<BaseEdge<BaseVertex>>> edges;
    /**
     * Stores Vertex hashed by source id. Good for O(1) search
     * Note that dest vertices are stored here as well with the isPart attribute set to something else
     */
    public HashMap<String, BaseVertex> vertices;

    public HashMapGraphStorage(BasePart part){
        super(part);
        edges = new HashMap<>();
        vertices = new HashMap<>();
    }


    @Override
    public BaseVertex addVertexMain(BaseVertex v) {
        return null;
    }

    @Override
    public BaseVertex addVertex(BaseVertex v) {
        // If vertex is already here then discard it
        if(vertices.containsKey(v.getId()))return null;
        v.setStorageCallback(this);
        vertices.put(v.getId(), v);
        return v;
    }

    @Override
    public BaseEdge<BaseVertex> addEdgeMain(BaseEdge<BaseVertex> e) {
        return null;
    }

    public boolean deleteVertex(BaseVertex v) {
        return false;
    }

//    @Override
//    @Deprecated
//    public void updateVertex(Feature f) {
//        if(!vertices.containsKey(f.attachedId))return;
//        getVertex(f.attachedId).updateFeatureCallback(c,f);
//    }

    @Override
    public BaseEdge<BaseVertex> addEdge(BaseEdge<BaseVertex> e) {
        // 1. If source vertex not in storage create it
        try{
            this.addVertex(e.source);
            this.addVertex(e.destination);
            // 2. Create Edge
            edges.putIfAbsent(e.source.getId(),new ArrayList<>());
            e.source = this.getVertex(e.source.getId());
            e.destination = this.getVertex(e.destination.getId());
            e.setStorageCallback(this);
            edges.get(e.source.getId()).add(e);
            // 3. Make Edge Callback & Return
            return e;
        }catch (NotFoundException ec){
            ec.printStackTrace();
        }
        return null;
    }

    @Override
    public void updateFeatureMain(Feature.Update<?> e) {

    }

    public void deleteEdge(BaseEdge<BaseVertex> e) {

    }

    @Override
    public void updateFeature(Feature.Update<?> e) {
        try{
            Class<?> featureClass = Class.forName(e.attachedToClassName);
            if(BaseVertex.class.isAssignableFrom(featureClass)){
                // Vertex feature
                BaseVertex vertex = this.getVertex(e.attachedId);
                if(vertex==null)return;
                vertex.updateFeatureCallback(e);
            }
            else if(BaseEdge.class.isAssignableFrom(featureClass)){
                // Edge feature
            }
        }catch(ClassNotFoundException ce){
           ce.printStackTrace();
        }catch (NullPointerException ne){
            ne.printStackTrace();
        }catch (NotFoundException ce){
            ce.printStackTrace();
        }
    }

    // Get Queries
    @Override
    public BaseVertex getVertex(String id) throws NotFoundException {
        return this.vertices.get(id);
    }

    @Override
    public BaseEdge<BaseVertex> getEdge(String source, String dest) throws NotFoundException {
        return null;
    }


    @Override
    public Stream<BaseVertex> getVertices() {
        return vertices.values().stream();
    }

    @Override
    public Stream<BaseEdge<BaseVertex>> getEdges() {
        return edges.values().stream().flatMap(Collection::stream);
    }
}
