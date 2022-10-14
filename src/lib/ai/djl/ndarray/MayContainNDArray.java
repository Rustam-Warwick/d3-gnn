package ai.djl.ndarray;

import java.util.function.Consumer;

public interface MayContainNDArray {

    void applyForNDArrays(Consumer<NDArray> operation);

}
