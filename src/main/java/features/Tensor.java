package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import scala.Tuple2;

public class Tensor extends Feature<Tuple2<NDArray, Integer>, NDArray> {

    public Tensor() {
    }

    public Tensor(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public Tensor(Tuple2<NDArray, Integer> value, boolean halo) {
        super(value, halo);
    }

    public Tensor(Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public Tensor(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public Tensor(String id, Tuple2<NDArray, Integer> value, boolean halo) {
        super(id, value, halo);
    }

    public Tensor(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        Tensor tmp = new Tensor(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
//        tmp.element = this.element;
        tmp.partId = this.partId;
//        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDArray copyArray = this.value._1.toDevice(this.value._1.getDevice(), true);

        Tensor tmp = new Tensor(this.id, new Tuple2<>(copyArray, this.value._2), this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return this.value._1;
    }

    public boolean isReady(int modelVersion){
        return true;
//        if (this.storage.isFirst()) {
//            return true;
//        }
//        return modelVersion == this.version;
    }

    @Override
    public boolean valuesEqual(Tuple2<NDArray, Integer> v1, Tuple2<NDArray, Integer> v2) {
        return false;
    }
}
