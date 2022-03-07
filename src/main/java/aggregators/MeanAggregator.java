package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashMap;

public class MeanAggregator extends BaseAggregator<Tuple3<NDArray, Integer, HashMap<Integer, Integer>>> {
    public MeanAggregator() {
        super();
    }

    public MeanAggregator(NDArray tensor, boolean halo) {
        this(new Tuple3<>(tensor, 0, new HashMap<>()), halo);
    }

    public MeanAggregator(Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value) {
        super(value);
    }

    public MeanAggregator(Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(value, halo);
    }

    public MeanAggregator(Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregator(String id, Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value) {
        super(id, value);
    }

    public MeanAggregator(String id, Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(id, value, halo);
    }

    public MeanAggregator(String id, Tuple3<NDArray, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        MeanAggregator tmp = new MeanAggregator(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        return this.copy();
    }

    @RemoteFunction
    @Override
    public void reduce(NDArray newElement, int count) {
        NDArray newTensor = (this.value._1().mul(this.value._2()).add(newElement)).div(this.value._2() + count);
        this.value = new Tuple3<>(newTensor, this.value._2() + count, this.value._3());
    }

    @Override
    public void bulkReduce(NDArray... newElements) {
        if(newElements.length <= 0) return;
        NDArray copyFirst = newElements[0].toDevice(newElements[0].getDevice(), true);
        newElements[0] = copyFirst;
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        Rpc.call(this, "reduce", sum, newElements.length);
    }

    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        NDArray difference = newElement.sub(oldElement);
        this.value._1().addi(difference.div(this.value._2()));
    }

    @Override
    public void bulkReplace(Tuple2<NDArray, NDArray> ...elements) {
        if(elements.length <= 0) return;
        NDArray copyFirstNew = elements[0]._1.toDevice(elements[0]._1.getDevice(), true);
        NDArray copyFirstOld = elements[0]._2.toDevice(elements[0]._2.getDevice(), true);
        elements[0] = new Tuple2<>(copyFirstNew, copyFirstOld);
        NDArray sumNew = Arrays.stream(elements).map(item->item._1).reduce(NDArray::addi).get();
        NDArray sumOld = Arrays.stream(elements).map(item->item._2).reduce(NDArray::addi).get();
        Rpc.call(this, "replace", sumNew, sumOld);

    }

    @Override
    public boolean isReady(int modelVersion) {
        return true;
    }

    @Override
    public void reset() {

    }

    @Override
    public NDArray getValue() {
        return this.value._1();
    }

    @Override
    public boolean valuesEqual(Tuple3<NDArray, Integer, HashMap<Integer, Integer>> v1, Tuple3<NDArray, Integer, HashMap<Integer, Integer>> v2) {
        return false;
    }
}