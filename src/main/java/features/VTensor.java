package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import scala.Tuple2;

/**
 * Versioned NDArray, Used to represent embeddings of specific model versions
 */

public class VTensor extends Feature<Tuple2<NDArray, Integer>, NDArray> {

    public VTensor() {
    }

    public VTensor(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public VTensor(Tuple2<NDArray, Integer> value, boolean halo) {
        super(value, halo);
    }

    public VTensor(Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public VTensor(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public VTensor(String id, Tuple2<NDArray, Integer> value, boolean halo) {
        super(id, value, halo);
    }

    public VTensor(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        VTensor tmp = new VTensor(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDArray copyArray = this.value._1.duplicate();
        VTensor tmp = new VTensor(this.id, new Tuple2<>(copyArray, this.value._2), this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return this.value._1;
    }

    public boolean isReady(int modelVersion) {
        if (this.storage.layerFunction.isFirst()) return true;
        return this.value._2 == modelVersion;
    }
}
