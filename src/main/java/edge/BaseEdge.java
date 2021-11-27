package edge;

import storage.GraphStorage;
import types.IncrementalAggregatable;
import types.GraphElement;
import vertex.BaseVertex;

abstract public class BaseEdge<VT extends BaseVertex> extends GraphElement implements IncrementalAggregatable {
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
