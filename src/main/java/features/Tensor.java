package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;

public class Tensor extends Feature<NDArray> {

    public Tensor(NDArray value) {
        super(value);
    }

    public Tensor(NDArray value, boolean halo) {
        super(value, halo);
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo) {
        super(id, value, halo);
    }

    @Override
    public NDArray getValue() {
        return (NDArray) this.value;
    }

    @Override
    public boolean valuesEqual(Object v1, Object v2) {
        return false;
    }
}
