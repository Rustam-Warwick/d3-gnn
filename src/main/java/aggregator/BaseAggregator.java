package aggregator;

import part.BasePart;
import types.GraphQuery;
import vertex.BaseVertex;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public abstract class BaseAggregator<VT extends BaseVertex> {
    public BasePart<VT> part = null;

    public BaseAggregator<VT> attachedTo(BasePart<VT> e){
        part =e;
        return this;

    }

    public BasePart<VT> getPart() {
        return part;
    }

    public void setPart(BasePart<VT> part) {
        this.part = part;
    }

    /**
     * Is the Ojects real type accepted in this aggregator. Does it care?
     * @param o Object to by typechecked
     * @return
     */
    public abstract boolean shouldTrigger(GraphQuery o);

    /**
     * If objects type is accepted then comes into dispatch
     * @param msg Object instance
     */
    public abstract void dispatch(GraphQuery msg);
}
