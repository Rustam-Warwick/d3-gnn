package storage;

import edge.BaseEdge;
import features.Feature;
import javassist.NotFoundException;
import part.BasePart;
import vertex.BaseVertex;

import java.util.stream.Stream;

public abstract class GraphStorage {
    public transient BasePart part = null;
    public GraphStorage(BasePart part){
        this.part = part;
    }
    public GraphStorage(){
        this.part = null;
    }
    public void setPart(BasePart part) {
        this.part = part;
    }

    public BasePart getPart() {
        return part;
    }
    // CRUD Related Stuff
    abstract public BaseVertex addVertex(BaseVertex v);
    abstract public boolean deleteVertex(BaseVertex v);
    abstract public BaseEdge<BaseVertex> addEdge(BaseEdge<BaseVertex> e);
    abstract public void deleteEdge(BaseEdge<BaseVertex> e);
    abstract public void updateFeature(Feature.Update<?> e);
    // Query Related Stuff
    abstract public BaseVertex getVertex(String id) throws NotFoundException;
    abstract public Stream<BaseVertex> getVertices();
    abstract public Stream<BaseEdge<BaseVertex>> getEdges();
    abstract public BaseEdge<BaseVertex> getEdge(String source,String dest) throws NotFoundException;


}
