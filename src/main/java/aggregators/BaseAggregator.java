package aggregators;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.iterations.RemoteFunction;

/**
 * Base class for all GNN Aggregators,
 * Stored value is a generic while the return valus in always an NDArray of the aggregations seen so far
 * @param <T> Values to actually store in this baseaggregator
 */
public abstract class BaseAggregator<T> extends Feature<T, NDArray> {
    public BaseAggregator() {
        super();
    }

    public BaseAggregator(BaseAggregator<T> agg, boolean deepCopy) {
        super(agg, deepCopy);
    }

    public BaseAggregator(T value) {
        super(value);
    }

    public BaseAggregator(T value, boolean halo, short master) {
        super(value, halo, master);
    }

    public BaseAggregator(String id, T value) {
        super(id, value);
    }

    public BaseAggregator(String id, T value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    /**
     * Given a new element reduce that to the new aggregator value
     */
    @RemoteFunction
    public abstract void reduce(NDList newElement, int count);

    /**
     * Called on an update on the element that was reduced before
     */
    @RemoteFunction
    public abstract void replace(NDList newElement, NDList oldElement);

    /**
     * @param aggGradient dLoss/dAgg
     * @return dLoss/dmessages
     */
    public abstract NDArray grad(NDArray aggGradient, NDList messages);

    /**
     * Reset the aggregator to its initial zero value state
     * Called after model update to start everything from scratch
     */
    public abstract void reset();
}
