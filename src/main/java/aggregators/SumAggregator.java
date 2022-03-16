package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import helpers.NDTensor;
import iterations.RemoteFunction;
import scala.Tuple3;

import java.util.HashMap;

public class SumAggregator extends BaseAggregator<Tuple3<NDTensor, Integer, HashMap<Integer, Integer>>> {
    public SumAggregator() {
        super();
    }

    public SumAggregator(NDArray tensor, boolean halo) {
        this(new Tuple3<>(new NDTensor(tensor), 0, new HashMap<>()), halo);
    }

    public SumAggregator(Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value) {
        super(value);
    }

    public SumAggregator(Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(value, halo);
    }

    public SumAggregator(Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public SumAggregator(String id, Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value) {
        super(id, value);
    }

    public SumAggregator(String id, Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(id, value, halo);
    }

    public SumAggregator(String id, Tuple3<NDTensor, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
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
        SumAggregator tmp = new SumAggregator(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(NDTensor newElement, int count) {
        NDArray res = this.value._1().get(this.storage.manager.getTempManager()).add(newElement.get(this.storage.manager.getTempManager()));
        this.value = new Tuple3<>(new NDTensor(res), this.value._2() + count, this.value._3());
        if(this.attachedTo._2.equals("434")){
            System.out.println("Reduce count: "+count+"  NumOfAggElements: "+this.value._2()+"  In Storage Position: "+this.storage.position);
        }

    }

    @Override
    public void bulkReduce(NDArray... newElements) {

    }

    @RemoteFunction
    @Override
    public void replace(NDTensor newElement, NDTensor oldElement) {
//        newElement.subi(oldElement);
//        this.value._1().addi(newElement);
//        this.value = new Tuple3<>(this.value._1(), this.value._2(), this.value._3());
    }


    @Override
    public NDArray grad() {
//        return this.value._1().getGradient();
        return null;
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
        return this.value._1().get(this.storage.manager.getTempManager());
    }

}