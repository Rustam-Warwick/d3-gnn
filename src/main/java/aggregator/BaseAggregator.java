package aggregator;

import part.BasePart;
import types.GraphQuery;
import vertex.BaseVertex;

/**
 * Base Aggregaation Function. Note that, repartitiioning can also be defined as a sort of graph aggregation
 *
 *
 */
public abstract class BaseAggregator {
    public BasePart part = null;

    public BaseAggregator attachedTo(BasePart e){
        this.part =e;
        return this;

    }

    public BasePart getPart() {
        return part;
    }

    public void setPart(BasePart part) {
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
