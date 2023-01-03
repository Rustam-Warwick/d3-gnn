package elements.features;

import ai.djl.ndarray.LifeCycleControl;
import ai.djl.ndarray.NDArray;

public class CountValueHolder implements LifeCycleControl {

    public NDArray val;

    public int count;

    public CountValueHolder(NDArray val, int count) {
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
}
