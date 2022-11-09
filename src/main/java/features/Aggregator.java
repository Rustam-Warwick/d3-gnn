package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;

public interface Aggregator {

    NDArray getValue();

    void reduce(NDList newElement, int count);

    void replace(NDList newElement, NDList oldElement);

    NDArray grad(NDArray aggGradient);

    void reset();

    int reducedCount();
}
