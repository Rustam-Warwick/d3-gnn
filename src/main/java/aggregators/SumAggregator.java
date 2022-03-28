package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class SumAggregator extends BaseAggregator<Tuple3<NDArray, Integer, HashMap<Short, Integer>>> {
    public SumAggregator() {
        super();
    }

    public SumAggregator(NDArray tensor, boolean halo) {
        this(new Tuple3<>(tensor, 0, new HashMap<>()), halo);
    }

    public SumAggregator(Tuple3<NDArray, Integer, HashMap<Short, Integer>> value) {
        super(value);
    }

    public SumAggregator(Tuple3<NDArray, Integer, HashMap<Short, Integer>> value, boolean halo) {
        super(value, halo);
    }

    public SumAggregator(Tuple3<NDArray, Integer, HashMap<Short, Integer>> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public SumAggregator(String id, Tuple3<NDArray, Integer, HashMap<Short, Integer>> value) {
        super(id, value);
    }

    public SumAggregator(String id, Tuple3<NDArray, Integer, HashMap<Short, Integer>> value, boolean halo) {
        super(id, value, halo);
    }

    public SumAggregator(String id, Tuple3<NDArray, Integer, HashMap<Short, Integer>> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        SumAggregator tmp = new SumAggregator(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        SumAggregator tmp = (SumAggregator) this.copy();
        tmp.element = this.element;
        tmp.storage = this.storage;
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(int version, short partId, NDArray newElement, int count) {
        Optional<Integer> maxVersion = this.value._3().values().stream().max(Integer::compare);

        if(maxVersion.isPresent() && maxVersion.get() < version){
            this.reset();
        }

        this.value._1().addi(newElement);
        this.value._3().put(partId, version);
        this.value = new Tuple3<>(this.value._1(), this.value._2() + count, this.value._3());
        if(this.attachedTo._2.equals("434")){
            System.out.println("Reduce count: "+count+"  NumOfAggElements: "+this.value._2()+"  In Storage Position: "+this.storage.position);
        }

    }

    @Override
    public void bulkReduce(int version, short partId, NDArray... newElements) {
        if(newElements.length <= 0) return;
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        Rpc.call(this, "reduce", version, partId, sum, newElements.length);
    }

    @RemoteFunction
    @Override
    public void replace(int version, short partId, NDArray newElement, NDArray oldElement) {
        newElement.subi(oldElement);
        this.value._1().addi(newElement);
        this.value._3().put(partId, version);
        this.value = new Tuple3<>(this.value._1(), this.value._2(), this.value._3());
    }

    @Override
    public NDArray grad() {
        return this.value._1().getGradient();
    }

    @Override
    public boolean isReady(int modelVersion) {
        Optional<Integer> maxVersion = this.value._3().values().stream().max(Integer::compare);
        Optional<Integer> minVersion = this.value._3().values().stream().min(Integer::compare);
        return maxVersion.orElse(0) == minVersion.orElse(0);
    }

    @Override
    public void reset() {
        this.value._1().subi(this.value._1());
        this.value = new Tuple3<>(value._1(), 0, value._3());
    }

    @Override
    public NDArray getValue() {
        return this.value._1();
    }
}