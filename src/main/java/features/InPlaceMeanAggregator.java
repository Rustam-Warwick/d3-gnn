package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphElement;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import storage.BaseStorage;

import java.util.function.Consumer;

/**
 * Mean aggregator that does inplace operation hence the updated oldValue is never used
 */
public final class InPlaceMeanAggregator extends Feature<Tuple2<NDArray, Integer>, NDArray> implements Aggregator {

    public InPlaceMeanAggregator() {
    }

    public InPlaceMeanAggregator(NDArray value) {
        super(Tuple2.of(value, 0));
    }

    public InPlaceMeanAggregator(NDArray value, boolean halo, short master) {
        super(Tuple2.of(value, 0), halo, master);
    }

    public InPlaceMeanAggregator(String id, NDArray value) {
        super(id, Tuple2.of(value, 0));
    }

    public InPlaceMeanAggregator(String id, NDArray value, boolean halo, short master) {
        super(id, Tuple2.of(value, 0), halo, master);
    }

    public InPlaceMeanAggregator(InPlaceMeanAggregator f, CopyContext context) {
        super(f, context);
    }

    public static NDArray bulkReduce(NDArray newMessages) {
        return newMessages.sum(new int[]{0});
    }

    @Override
    public InPlaceMeanAggregator copy(CopyContext context) {
        return new InPlaceMeanAggregator(this, context);
    }

    @Override
    public Consumer<BaseStorage> createElement() {
        Consumer<BaseStorage> tmp = super.createElement();
        if (storage.needsTensorDelay() && tmp != null) value.f0.delay();
        return tmp;
    }

    @Override
    public Tuple2<Consumer<BaseStorage>, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        Tuple2<Consumer<BaseStorage>, GraphElement> callback = super.updateElement(newElement, memento);
        InPlaceMeanAggregator mementoAggregator = (InPlaceMeanAggregator) callback.f1;
        if (storage.needsTensorDelay() && callback.f0 != null && mementoAggregator.value.f0 != value.f0) {
            value.f0.delay();
            mementoAggregator.value.f0.resume();
        }
        return callback;
    }

    @RemoteFunction
    @Override
    public void reduce(NDList newElement, int count) {
        value.f0.addi(newElement.get(0));
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
