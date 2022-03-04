package aggregators;

import ai.djl.engine.Engine;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.Feature;
import elements.GraphElement;
import features.Set;
import scala.Int;
import scala.Tuple3;

import java.util.HashMap;
import java.util.List;

public class MeanAggregator extends BaseAggregator<Tuple3<NDArray, Integer, HashMap<Integer, Integer>>>{
    public MeanAggregator() {
        super();
    }

    public MeanAggregator(NDArray tensor, boolean halo){
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

    @Override
    public void reduce(NDArray newElement, int count) {

    }

    @Override
    public void bulkReduce(NDArray... newElements) {

    }

    @Override
    public void replace(NDArray newElement, NDArray oldElement) {

    }

    @Override
    public void bulkReplace() {

    }

    @Override
    public boolean isReady() {
        return false;
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