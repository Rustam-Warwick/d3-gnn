package aggregators;

import ai.djl.ndarray.NDArray;
import iterations.RemoteFunction;
import scala.Tuple2;

import java.util.Arrays;

public class NewMeanAggregator extends BaseAggregator<Tuple2<NDArray, Integer>> {

    public NewMeanAggregator() {
    }

    public NewMeanAggregator(NDArray value, boolean halo){
        this(new Tuple2<>(value, 0), halo, (short) -1);
    }

    public NewMeanAggregator(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public NewMeanAggregator(Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public NewMeanAggregator(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public NewMeanAggregator(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public NewMeanAggregator copy() {
        NewMeanAggregator tmp = new NewMeanAggregator(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public NewMeanAggregator deepCopy() {
        NewMeanAggregator tmp = this.copy();
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        tmp.element = this.element;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(NDArray newElement, int count) {
        this.value._1().muli(this.value._2()).addi(newElement).divi(this.value._2() + count);
        int newCount = this.value._2() + count;
        this.value = new Tuple2<>(this.value._1(), newCount);
    }
    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        newElement.subi(oldElement).divi(value._2());
        value._1().addi(newElement);
    }

    @Override
    public NDArray grad() {
        return getValue().getGradient().div(this.value._2());
    }

    @Override
    public NDArray getValue() {
        return value._1;
    }

    @Override
    public boolean isReady(int modelVersion) {
        new Exception("No Use").printStackTrace();
        return false;
    }

    @Override
    public void reset() {
        value = new Tuple2<>(value._1, 0);
    }
    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        return sum;
    }
}
