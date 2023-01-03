package elements.features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.enums.CopyContext;

/**
 * Feature of {@link NDArray} Used to represent embeddings of specific model versions
 */
public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
        super();
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo) {
        super(id, value, halo);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public Tensor(Feature<NDArray, NDArray> f, CopyContext context) {
        super(f, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tensor copy(CopyContext context) {
        return new Tensor(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray getValue() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return v1 == v2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        value.delay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        value.resume();
    }
}