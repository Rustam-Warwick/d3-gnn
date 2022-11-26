package elements.features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;

/**
 * Special interface to be implemented by all {@link ai.djl.nn.gnn.AggregatorVariant}s {@link elements.Feature}
 */
public interface Aggregator {

    /**
     * Get value of aggregator should be the {@link NDArray} despite it actually storing any {@link Object}
     */
    NDArray getValue();

    /**
     * When a new connection arrives reduce it to the given aggregator
     *
     * @implNote Multiple reduce messages can be batched and then sent over which is represented in @param count
     */
    void reduce(NDList newElement, int count);

    /**
     * When an old connection is updated reduce message is triggered with oldValue and newValue
     */
    void replace(NDList newElement, NDList oldElement);

    /**
     * Given Dl/Da return dl/dmessage
     */
    NDArray grad(NDArray aggGradient);

    /**
     * Reset this aggregator to its initial state
     */
    void reset();

    /**
     * Return the number of reduced connections to this aggregator
     */
    int reducedCount();
}
