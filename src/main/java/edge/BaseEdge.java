package edge;

import types.GraphElement;
import vertex.BaseVertex;

import java.util.concurrent.CompletableFuture;

abstract public class BaseEdge<VT extends BaseVertex> extends GraphElement  {
    public VT source;
    public VT destination;

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

    public BaseEdge(BaseEdge<VT> e){
        super(e);
        this.source = (VT) e.source.copy();
        this.destination = (VT) e.destination.copy();
    }
    abstract public BaseEdge<VT> copy();
}
