package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.enums.CopyContext;

/**
 * Special interface to be implemented by all {@link ai.djl.nn.gnn.AggregatorVariant}s {@link elements.Feature}
 */
public abstract class Aggregator<T> extends Feature<T, NDArray> {

    public Aggregator() {
        super();
    }

    public Aggregator(String name, T value) {
        super(name, value);
    }

    public Aggregator(String name, T value, boolean halo) {
        super(name, value, halo);
    }

    public Aggregator(String name, T value, boolean halo, short master) {
        super(name, value, halo, master);
    }

    public Aggregator(Feature<T, NDArray> feature, CopyContext context) {
        super(feature, context);
    }

    /**
     * When a new connection arrives reduce it to the given aggregator
     *
     * @implNote Multiple reduce messages can be batched and then sent over which is represented in @param count
     */
    public abstract void reduce(NDList newElement, int count);

    /**
     * When an old connection is updated reduce output is triggered with oldValue and newValue
     */
    public abstract void replace(NDList newElement, NDList oldElement);

    /**
     * Reset this aggregator to its initial state
     */
    public abstract void reset();

    /**
     * Return the number of reduced connections to this aggregator
     */
    public abstract int getReducedCount();
}
