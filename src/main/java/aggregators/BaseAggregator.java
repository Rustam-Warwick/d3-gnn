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
    public abstract void reduce(int version, NDArray newElement, int count);

    @RemoteFunction
    public abstract void replace(int version, NDArray newElement, NDArray oldElement);

    public abstract NDArray grad();

    public abstract boolean isReady(int modelVersion);

    public abstract void reset();
}
