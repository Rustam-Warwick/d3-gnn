package storage;

import edge.BaseEdge;
import features.Feature;
import part.BasePart;
import vertex.BaseVertex;

import java.util.stream.Stream;

public abstract class GraphStorage<VT extends BaseVertex> {
    public transient BasePart<VT> part = null;
    public GraphStorage(BasePart<VT> part){
        this.part = part;
    }
    public GraphStorage(){
        this.part = null;
    }
    public void setPart(BasePart<VT> part) {
        this.part = part;
    }

    public BasePart<VT> getPart() {
        return part;
    }

    // CRUD Related Stuff
    abstract public VT addVertex(VT v);
    abstract public boolean deleteVertex(VT v);
    abstract public BaseEdge<VT> addEdge(BaseEdge<VT> e);
    abstract public void deleteEdge(BaseEdge<VT> e);
    abstract public void updateFeature(Feature.Update<?> e);
    // Query Related Stuff
    abstract public VT getVertex(String id);
    abstract public Stream<VT> getVertices();
    abstract public Stream<BaseEdge<VT>> getEdges();
    abstract public BaseEdge<VT> getEdge();


}
