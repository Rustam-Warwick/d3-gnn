package elements.features;

import ai.djl.ndarray.LifeCycleControl;
import ai.djl.ndarray.NDArray;

/**
 * Tensor with count holder for aggregators
 */
public class CountTensorHolder implements LifeCycleControl {

    public NDArray val;

    public int count;

    public CountTensorHolder(NDArray val, int count) {
        this.val = val;
        this.count = count;
    }

    @Override
    public void delay() {
        val.delay();
    }

    @Override
    public void resume() {
        val.resume();
    }

    @Override
    public void destroy() {
        val.destroy();
    }

    @Override
    public String toString() {
        return String.valueOf(count);
    }
}
