package aggregators;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import iterations.RemoteFunction;

public abstract class BaseAggregator<T> extends Feature<T, NDArray> {
    public BaseAggregator() {
        super();
    }

    public BaseAggregator(T value) {
        super(value);
    }

    public BaseAggregator(T value, boolean halo) {
        super(value, halo);
    }

    public BaseAggregator(T value, boolean halo, short master) {
        super(value, halo, master);
    }

    public BaseAggregator(String id, T value) {
        super(id, value);
    }

    public BaseAggregator(String id, T value, boolean halo) {
        super(id, value, halo);
    }

    public BaseAggregator(String id, T value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public abstract void reduce(NDArray newElement, int count);
    public abstract void bulkReduce(NDArray ...newElements);
    public abstract void replace(NDArray newElement, NDArray oldElement);
    public abstract void bulkReplace();
    public abstract boolean isReady();
    public abstract void reset();
}
