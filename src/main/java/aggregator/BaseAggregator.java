package aggregator;

import edge.BaseEdge;
import part.BasePart;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public abstract class BaseAggregator<VT extends BaseVertex, ET extends BaseEdge<VT>> {
    public GraphStorage storage = null;

    public GraphStorage getStorage() {
        return storage;
    }

    public void setStorage(GraphStorage storage) {
        this.storage = storage;
    }

    public BaseAggregator attachedTo(GraphStorage e){
        this.storage =e;
        return this;
    }
    public void preAddVertexCallback(VT vertex){

    }
    public void addVertexCallback(VT vertex){

    }

    public void preAddEdgeCallback(ET edge){

    }
    public void addEdgeCallback(ET edge){

    }
    public void preUpdateFeatureCallback(){

    }
    public void updateVertexCallback(VT vertex){

    }
    public void dispatch(GraphQuery query){

    }

}
