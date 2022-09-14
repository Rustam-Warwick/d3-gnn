package aggregators;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.iterations.RemoteFunction;

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

    @RemoteFunction
    public abstract void reduce(NDList newElement, int count);

    @RemoteFunction
    public abstract void replace(NDList newElement, NDList oldElement);

    /**
     * @param aggGradient dLoss/dAgg
     * @return dLoss/dmessages
     */
    public abstract NDArray grad(NDArray aggGradient, NDList messages);

    public abstract void reset();
}
