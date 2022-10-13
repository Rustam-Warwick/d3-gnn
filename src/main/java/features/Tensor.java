package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.Consumer;

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

    public Tensor(String id, NDArray value, boolean halo, Short master) {
        super(id, value, halo, master);
    }

    public Tensor(NDArray value, boolean halo, Short master) {
        super(value, halo, master);
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

    @Override
    public Consumer<Plugin> createElement() {
        Consumer<Plugin> tmp = super.createElement();
        if (tmp != null) value.postpone();
        return tmp;
    }

    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Tuple2<Consumer<Plugin>, GraphElement> callback = super.updateElement(newElement, memento);
        Tensor mementoAggregator = (Tensor) callback.f1;
        if (callback.f0 != null && mementoAggregator.value != value) {
            value.postpone();
            mementoAggregator.value.prepone();
        }
        return callback;
    }

    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return v1 == v2;
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        operation.accept(value);
    }

}