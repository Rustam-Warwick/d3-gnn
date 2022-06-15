package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;

/**
 * Versioned NDArray, Used to represent embeddings of specific model versions
 */

public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
        super();
    }

    public Tensor(Tensor s, boolean deepCopy) {
        super(s, deepCopy);
    }

    public Tensor(NDArray value) {
        super(value);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public Tensor copy() {
        return new Tensor(this, false);
    }

    @Override
    public Tensor deepCopy() {
        return new Tensor(this, true);
    }

    @Override
    public NDArray getValue() {
        return this.value;
    }

}
