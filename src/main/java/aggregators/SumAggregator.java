package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import elements.iterations.RemoteFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.function.Consumer;

public class SumAggregator extends BaseAggregator<NDArray> {

    public SumAggregator() {
        super();
    }

    public SumAggregator(BaseAggregator<NDArray> agg, boolean deepCopy) {
        super(agg, deepCopy);
    }

    public SumAggregator(NDArray value) {
        super(value);
    }

    public SumAggregator(NDArray value, boolean halo, short master) {
        super(value, halo, master);
    }

    public SumAggregator(String id, NDArray value) {
        super(id, value);
    }

    public SumAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::add).get();
        return sum;
    }

    @Override
    public SumAggregator copy() {
        return new SumAggregator(this, false);
    }

    @Override
    public SumAggregator deepCopy() {
        return new SumAggregator(this, true);
    }

    @RemoteFunction
    @Override
    public void reduce(NDArray newElement, int count) {
        value.addi(newElement);
    }

    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        value.subi(oldElement).addi(newElement)
    }

    @Override
    public NDArray grad(NDArray aggGradient, NDArray messages) {
        return aggGradient.expandDims(0).repeat(0, messages.getShape().get(0)); // (batch_size, gradient)
    }

    @Override
    public NDArray getValue() {
        return value;
    }

    @Override
    public void reset() {
        value = value.zerosLike();
        storage.updateFeature(this);
    }

    @Override
    public Boolean createElement() {
        value.postpone();
        return super.createElement();
    }

    @Override
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        SumAggregator tmp = (SumAggregator) newElement;
        if (value != tmp.value) {
            value.prepone();
            tmp.value.postpone();
        }
        return super.updateElement(newElement, memento);
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        operation.accept(value);
    }

    @Override
    public void applyForNDArray(Consumer<NDArray> operation) {
        super.applyForNDArray(operation);
        operation.accept(value);
    }
}
