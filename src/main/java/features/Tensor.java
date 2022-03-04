package features;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.Feature;
import elements.GraphElement;
import storage.BaseStorage;

import java.util.HashSet;

public class Tensor extends Feature<NDArray, NDArray> {
    public int version = 0;
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
        tmp.version = this.version;
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDArray emptyArray = BaseStorage.tensorManager.create(this.value.getShape());
        this.value.copyTo(emptyArray);
        Tensor tmp = new Tensor(this.id, emptyArray, this.halo, this.master);
        tmp.version = this.version;
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.putAll(this.features);
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return this.value;
    }

    @Override
    public boolean valuesEqual(NDArray v1, NDArray v2) {
        return false;
    }
}
