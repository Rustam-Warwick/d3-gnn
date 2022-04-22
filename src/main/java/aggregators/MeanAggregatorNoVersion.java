package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import iterations.RemoteFunction;
import scala.Tuple4;

import java.util.Arrays;
import java.util.Objects;

/**
 * Value, Count of Aggregated Values, Total Neighbors, Version
 */
public class MeanAggregatorNoVersion extends BaseAggregator<Tuple4<NDArray, Integer, Integer, Integer>> {
    public MeanAggregatorNoVersion() {
        super();
    }

    public MeanAggregatorNoVersion(NDArray tensor, boolean halo) {
        this(new Tuple4<>(tensor, 0, 0, 0), halo);
    }

    public MeanAggregatorNoVersion(Tuple4<NDArray, Integer, Integer, Integer> value) {
        super(value);
    }

    public MeanAggregatorNoVersion(Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo) {
        super(value, halo);
    }

    public MeanAggregatorNoVersion(Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregatorNoVersion(String id, Tuple4<NDArray, Integer, Integer, Integer> value) {
        super(id, value);
    }

    public MeanAggregatorNoVersion(String id, Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo) {
        super(id, value, halo);
    }

    public MeanAggregatorNoVersion(String id, Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        return sum;
    }

    @Override
    public GraphElement copy() {
        MeanAggregatorNoVersion tmp = new MeanAggregatorNoVersion(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        MeanAggregatorNoVersion tmp = (MeanAggregatorNoVersion) this.copy();
        tmp.element = this.element;
        tmp.storage = this.storage;
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(int version, NDArray newElement, int count) {
        this.value._1().muli(this.value._2()).addi(newElement).divi(this.value._2() + count);
        int newCount = this.value._2() + count;
        this.value = new Tuple4<>(this.value._1(), newCount, Math.max(this.value._3(), newCount), version);
        if (this.attachedTo._2.equals("10")) {
            System.out.println("Reduce count: " + count + "From part: " + partId + "  NumOfAggElements: " + this.value._2() + "  In Storage Position: " + this.storage.layerFunction.getPosition());
        }
    }

    @RemoteFunction
    @Override
    public void replace(int version, NDArray newElement, NDArray oldElement) {
        newElement.subi(oldElement).divi(value._2());
        value._1().addi(newElement);
    }

    @Override
    public NDArray grad() {
        return this.value._1().getGradient().div(this.value._2());
    }

    @Override
    public boolean isReady(int modelVersion) {
        return true;
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