package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import iterations.RemoteFunction;
import iterations.Rmi;
import scala.Tuple4;

import java.util.Arrays;
import java.util.Objects;

public class MeanAggregator extends BaseAggregator<Tuple4<NDArray, Integer, Integer, Integer>> {
    public MeanAggregator() {
        super();
    }

    public MeanAggregator(NDArray tensor, boolean halo) {
        this(new Tuple4<>(tensor, 0, 0, 0), halo);
    }

    public MeanAggregator(Tuple4<NDArray, Integer, Integer, Integer> value) {
        super(value);
    }

    public MeanAggregator(Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo) {
        super(value, halo);
    }

    public MeanAggregator(Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregator(String id, Tuple4<NDArray, Integer, Integer, Integer> value) {
        super(id, value);
    }

    public MeanAggregator(String id, Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo) {
        super(id, value, halo);
    }

    public MeanAggregator(String id, Tuple4<NDArray, Integer, Integer, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        MeanAggregator tmp = new MeanAggregator(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        MeanAggregator tmp = (MeanAggregator) this.copy();
        tmp.element = this.element;
        tmp.storage = this.storage;
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(int version, short partId, NDArray newElement, int count) {
        if (version < this.value._4()) return;
        if (version > this.value._4()) reset();
        this.value._1().muli(this.value._2()).addi(newElement).divi(this.value._2() + count);
        int newCount = this.value._2() + count;
        this.value = new Tuple4<>(this.value._1(), newCount, Math.max(this.value._3(), newCount), version);
        if (this.attachedTo._2.equals("10")) {
            System.out.println("Reduce count: " + count + "  NumOfAggElements: " + this.value._2() + "  In Storage Position: " + this.storage.layerFunction.getPosition());
        }
    }

    @Override
    public void bulkReduce(int version, short partId, NDArray... newElements) {
        if (newElements.length <= 0) return;
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        Rmi.call(this, "reduce", version, partId, sum, newElements.length);
    }

    @RemoteFunction
    @Override
    public void replace(int version, short partId, NDArray newElement, NDArray oldElement) {
        if (version < this.value._4()) return;
        NDArray difference = newElement.sub(oldElement);
        this.value._1().addi(difference.div(this.value._2()));
    }

    @Override
    public NDArray grad() {
        return this.value._1().getGradient().divi(this.value._2());
    }

    @Override
    public boolean isReady(int modelVersion) {
        return Objects.equals(value._2(), value._3()) && modelVersion == this.value._4();
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