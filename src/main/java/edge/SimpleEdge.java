package edge;


import part.BasePart;
import storage.GraphStorage;
import vertex.BaseVertex;

public class SimpleEdge<VT extends BaseVertex>  extends BaseEdge<VT>{

    public SimpleEdge(String id, GraphStorage part, VT source, VT destination) {
        super(id, part, source, destination);
    }

    public SimpleEdge(String id, VT source, VT destination) {
        super(id, source, destination);
    }

    public SimpleEdge(VT source, VT destination) {
        super(source, destination);
    }

    public SimpleEdge() {
        super();
    }

    @Override
    public void addEdgeCallback() {
//        System.out.format("Edge added with part id %s \n",this.partId);
    }

    @Override
    public BaseEdge<VT> copy() {
        return this;
    }
}
