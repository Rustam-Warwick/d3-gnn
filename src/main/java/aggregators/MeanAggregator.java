package aggregators;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.GraphElement;
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
    public void reduce(NDArray newElement, int count) {
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start(new NDList(value.f0, newElement))) {
            this.value.f0.muli(this.value.f1).addi(newElement).divi(this.value.f1 + count);
            this.value.f1 += count;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start(new NDList(value.f0, newElement, oldElement))) {
            value.f0.addi((newElement.sub(oldElement)).divi(value.f1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public NDArray grad(NDArray aggGradient, NDArray messages) {
        return aggGradient.div(value.f1).expandDims(0).repeat(0, messages.getShape().get(0)); // (batch_size, gradient)
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
    public Boolean createElement() {
        value.f0.detach();
        return super.createElement();
    }

    @Override
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement, GraphElement memento) {
        MeanAggregator tmp = (MeanAggregator) newElement;
        if(value.f0 != tmp.value.f0){
            value.f0.attach(LifeCycleNDManager.getInstance());
            tmp.value.f0.detach();
        }
        return super.updateElement(newElement, memento);
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        operation.accept(value.f0);
    }

    @Override
    public void applyForNDArray(Consumer<NDArray> operation) {
        super.applyForNDArray(operation);
        operation.accept(value.f0);
    }
}
