package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.Feature;
import elements.GraphElement;

import java.util.HashSet;

public class Tensor extends Feature<NDArray> {

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
        Tensor tmp = new Tensor(this.id, (NDArray) this.value, this.halo, this.master);
        tmp.setPartId(this.getPartId());
        tmp.setStorage(this.storage);
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDArray emptyArray = NDManager.newBaseManager().create(((NDArray) this.value).getShape());
        ((NDArray) this.value).copyTo(emptyArray);
        Tensor tmp = new Tensor(this.id, emptyArray, this.halo, this.master);
        tmp.setPartId(this.getPartId());
        tmp.setStorage(this.storage);
        tmp.features.putAll(this.features);
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return (NDArray) this.value;
    }

    @Override
    public boolean valuesEqual(Object v1, Object v2) {
        return false;
    }
}
