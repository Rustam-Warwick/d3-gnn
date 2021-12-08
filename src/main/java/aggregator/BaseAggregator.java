package aggregator;

import edge.BaseEdge;
import part.BasePart;
import vertex.BaseVertex;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public abstract class BaseAggregator<VT extends BaseVertex, ET extends BaseEdge<VT>> {
    public BasePart part = null;

    public BaseAggregator attachedTo(BasePart e){
        this.part =e;
        return this;

    }
    public void addVertexCallback(VT vertex){

    }

    public void addEdgeCallback(ET edge){

    }

    public void updateVertexCallback(VT vertex){

    }



    public BasePart getPart() {
        return part;
    }

    public void setPart(BasePart part) {
        this.part = part;
    }
}
