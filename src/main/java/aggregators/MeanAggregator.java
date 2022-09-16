package aggregators;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.GraphElement;
import elements.Plugin;
import elements.iterations.RemoteFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.function.Consumer;

public class MeanAggregator extends BaseAggregator<Tuple2<NDArray, Integer>> {

    public MeanAggregator() {
        super();
    }

    public MeanAggregator(MeanAggregator m, boolean deepCopy) {
        super(m, deepCopy);
    }

    public MeanAggregator(NDArray value, boolean halo) {
        this(new Tuple2<>(value, 0), halo, (short) -1);
    }

    public MeanAggregator(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public MeanAggregator(Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregator(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public MeanAggregator(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::add).get();
        return sum;
    }

    @Override
    public MeanAggregator copy() {
        return new MeanAggregator(this, false);
    }

    @Override
    public MeanAggregator deepCopy() {
        return new MeanAggregator(this, true);
    }

    @RemoteFunction
    @Override
    public void reduce(NDList newElement, int count) {
        this.value.f0.muli(this.value.f1).addi(newElement.get(0)).divi(this.value.f1 + count);
        value.f1 += count;
    }

    @RemoteFunction
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.f0.addi((newElement.get(0).sub(oldElement.get(0))).divi(value.f1));
    }

    @Override
    public NDArray grad(NDArray aggGradient, NDList messages) {
        return aggGradient.div(value.f1).expandDims(0).repeat(0, messages.get(0).getShape().get(0)); // (batch_size, gradient)
    }

    @Override
    public NDArray getValue() {
        return value.f0;
    }

    @Override
    public void reset() {
        value = new Tuple2<>(value.f0.zerosLike(), 0);
        storage.updateFeature(this);
    }

    @Override
    public Consumer<Plugin> createElement() {
        Consumer<Plugin> tmp = super.createElement();
        if(tmp != null) value.f0.postpone();
        return tmp;
    }

    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Tuple2<Consumer<Plugin>, GraphElement> callback = super.updateElement(newElement, memento);
        MeanAggregator mementoAggregator = (MeanAggregator) callback.f1;
        if(callback.f0 != null && mementoAggregator.value.f0 != value.f0){
            value.f0.prepone();
            mementoAggregator.value.f0.postpone();
        }
        return callback;
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        operation.accept(value.f0);
    }

}
