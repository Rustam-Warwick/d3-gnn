package aggregators;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.iterations.RemoteFunction;

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
    public abstract void reduce(NDArray newElement, int count);

    @RemoteFunction
    public abstract void replace(NDArray newElement, NDArray oldElement);

    /**
     *
     * @param aggGradient dLoss/dAgg
     * @param message Edge message to this aggregator. (Batch_size, message_size)
     * @return dLoss/dmessages
     */
    public abstract NDArray grad(NDArray aggGradient, NDArray message);

    public abstract boolean isReady(int modelVersion);

    public abstract void reset();
}
