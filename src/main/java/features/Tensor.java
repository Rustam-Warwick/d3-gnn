package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import scala.Tuple2;

public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
    }

    public Tensor(NDArray value) {
        super(value);
    }

    public Tensor(NDArray value, boolean halo) {
        super(value, halo);
    }

    public Tensor(NDArray value, boolean halo, short master) {
        super(value, halo, master);
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo) {
        super(id, value, halo);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        Tensor tmp = new Tensor(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDArray copyArray = this.value.toDevice(this.value.getDevice(), true);
        Tensor tmp = new Tensor(this.id, copyArray, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return this.value;
    }

    public boolean isReady(int modelVersion){
        return true;
    }

    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return v1==v2;
    }
}
