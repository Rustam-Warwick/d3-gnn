package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;

/**
 * Versioned NDArray, Used to represent embeddings of specific model versions
 */

public class Tensor extends Feature<NDArray, NDArray> {

    public Tensor() {
        super();
    }

    public Tensor(NDArray value) {
        super(value);
    }

    public Tensor(String id, NDArray value) {
        super(id, value);
    }

    public Tensor(String id, NDArray value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public Tensor copy() {
        Tensor tmp = new Tensor(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Tensor deepCopy() {
        NDArray copyArray = this.value.duplicate();
        Tensor tmp = new Tensor(this.id, copyArray, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    /**
     * Additionally attach all Tensor to lifeCycleManager
     */
    @Override
    public Boolean createElement() {
        Boolean isCreated = super.createElement();
        if(isCreated) getValue().attach(storage.manager.getLifeCycleManager()); // Always attach to life cycle manager
        return isCreated;
    }

    @Override
    public NDArray getValue() {
        return this.value;
    }

}
