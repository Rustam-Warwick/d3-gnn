package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.iterations.RemoteFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.Consumer;

public final class MeanAggregator extends Feature<Tuple2<NDArray, Integer>, NDArray> implements Aggregator {

    public MeanAggregator() {
    }

    public MeanAggregator(NDArray value) {
        super(Tuple2.of(value, 0));
    }

    public MeanAggregator(NDArray value, boolean halo, short master) {
        super(Tuple2.of(value, 0), halo, master);
    }

    public MeanAggregator(String id, NDArray value) {
        super(id, Tuple2.of(value, 0));
    }

    public MeanAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, Tuple2.of(value, 0), halo, master);
    }

    public MeanAggregator(Feature<Tuple2<NDArray, Integer>, NDArray> f, boolean deepCopy) {
        super(f, deepCopy);
    }

    public static NDArray bulkReduce(NDArray newMessages) {
        return newMessages.sum(new int[]{0});
    }

    @Override
    public MeanAggregator copy() {
        return new MeanAggregator(this, false);
    }

    @Override
    public MeanAggregator deepCopy() {
        return new MeanAggregator(this, true);
    }

    @Override
    public Consumer<Plugin> createElement() {
        Consumer<Plugin> tmp = super.createElement();
        if (storage.requiresTensorDelay() && tmp != null) value.f0.delay();
        return tmp;
    }

    @Override
    public Tuple2<Consumer<Plugin>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Tuple2<Consumer<Plugin>, GraphElement> callback = super.updateElement(newElement, memento);
        MeanAggregator mementoAggregator = (MeanAggregator) callback.f1;
        if (storage.requiresTensorDelay() && callback.f0 != null && mementoAggregator.value.f0 != value.f0) {
            value.f0.delay();
            mementoAggregator.value.f0.resume();
        }
        return callback;
    }

    @RemoteFunction
    @Override
    public void reduce(NDList newElement, int count) {
        this.value.f0.addi(newElement.get(0));
        value.f1 += count;
    }

    @RemoteFunction
    @Override
    public void replace(NDList newElement, NDList oldElement) {
        value.f0.addi((newElement.get(0).sub(oldElement.get(0))));
    }

    @Override
    public NDArray grad(NDArray aggGradient) {
        return aggGradient.div(value.f1);
    }

    @Override
    public NDArray getValue() {
        return value.f0;
    }

    @Override
    public void reset() {
        value.f0.subi(value.f0);
        value.f1 = 0;
    }

    @Override
    public int reducedCount() {
        return value.f1;
    }

    @Override
    public void delay() {
        super.delay();
        value.f0.delay();
    }

    @Override
    public void resume() {
        super.resume();
        value.f0.resume();
    }

    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return Types.TUPLE(TypeInformation.of(NDArray.class), Types.INT);
    }
}
