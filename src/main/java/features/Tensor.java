package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.Consumer;

/**
 * Versioned NDArray, Used to represent embeddings of specific model versions
 */

public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
    }

    public Tensor(NDArray value) {
        super(value);
    }

    public Tensor(NDArray value, boolean halo, short master) {
        super(value, halo, master);
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public Tensor(Feature<NDArray, NDArray> f, boolean deepCopy) {
        super(f, deepCopy);
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
    public Consumer<Plugin> createElement() {
        Consumer<Plugin> callback = super.createElement();
        if (storage.requiresTensorDelay() && callback != null) value.delay();
        return callback;
    }

    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Tuple2<Consumer<Plugin>, GraphElement> callback = super.updateElement(newElement, memento);
        Tensor mementoAggregator = (Tensor) callback.f1;
        if (storage.requiresTensorDelay() && callback.f0 != null && mementoAggregator.value != value) {
            value.delay();
            mementoAggregator.value.resume();
        }
        return callback;
    }

    @Override
    public NDArray getValue() {
        return this.value;
    }

    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return v1 == v2;
    }

    @Override
    public void delay() {
        super.delay();
        value.delay();
    }

    @Override
    public void resume() {
        super.resume();
        value.resume();
    }

    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return TypeInformation.of(NDArray.class);
    }
}