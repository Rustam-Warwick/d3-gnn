package edge;

import part.BasePart;
import storage.GraphStorage;
import types.GraphElement;
import vertex.BaseVertex;

abstract public class BaseEdge<VT extends BaseVertex> extends GraphElement {
    public VT source;
    public VT destination;

    public BaseEdge(String id, GraphStorage part, VT source, VT destination) {
        super(id, part);
        this.source = source;
        this.destination = destination;
    }

    public BaseEdge(String id, VT source, VT destination) {
        super(id);
        this.source = source;
        this.destination = destination;
    }

    public BaseEdge(VT source, VT destination) {
        super(source.getId());
        this.source = source;
        this.destination = destination;
    }

    public BaseEdge(){
        super();
        this.source=null;
        this.destination=null;
    }

    abstract public void addEdgeCallback();
    abstract public BaseEdge<VT> copy();
}
