package aggregators;

import ai.djl.ndarray.NDArray;
import iterations.RemoteFunction;
import scala.Tuple4;

import java.util.Arrays;
import java.util.Objects;

/**
 * Value, Count of Aggregated Values, Total Neighbors, Version
 */
public class MeanAggregator extends BaseAggregator<Tuple4<NDArray, Integer, Integer, Integer>> {
    public MeanAggregator() {
        super();
    }

    public MeanAggregator(NDArray tensor, boolean halo) {
        this(new Tuple4<>(tensor, 0, 0, 0), halo, (short) -1);
    }

    public MeanAggregator(Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregator(String id, Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        return sum;
    }

    @Override
    public MeanAggregator copy() {
        MeanAggregator tmp = new MeanAggregator(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public MeanAggregator deepCopy() {
        MeanAggregator tmp = this.copy();
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
    public void reduce(int version, NDArray newElement, int count) {
        if (version < this.value._4()) return;
        if (version > this.value._4()) reset();
        this.value._1().muli(this.value._2()).addi(newElement).divi(this.value._2() + count);
        int newCount = this.value._2() + count;
        this.value = new Tuple4<>(this.value._1(), newCount, Math.max(this.value._3(), newCount), version);
    }

    @RemoteFunction
    @Override
    public void replace(int version, NDArray newElement, NDArray oldElement) {
        if (version < this.value._4()) return;
        newElement.subi(oldElement).divi(value._2());
        value._1().addi(newElement);
    }

    @Override
    public NDArray grad() {
        return this.value._1().getGradient().div(this.value._2());
    }

    @Override
    public boolean isReady(int modelVersion) {
        return Objects.equals(value._2(), value._3()) && (modelVersion == this.value._4() || value._2() == 0);
    }

    @Override
    public void reset() {
        this.value = new Tuple4<>(this.value._1(), 0, this.value._3(), this.value._4());
    }

    @Override
    public NDArray getValue() {
        return this.value._1();
    }

}