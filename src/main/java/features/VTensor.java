package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Versioned NDArray, Used to represent embeddings of specific model versions
 */

public class VTensor extends Feature<Tuple2<NDArray, Integer>, NDArray> {

    public VTensor() {
        super();
    }

    public VTensor(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public VTensor(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public VTensor(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public VTensor copy() {
        VTensor tmp = new VTensor(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public VTensor deepCopy() {
        NDArray copyArray = this.value.f0.duplicate();
        VTensor tmp = new VTensor(this.id, new Tuple2<>(copyArray, this.value.f1), this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    @Override
    public NDArray getValue() {
        return this.value.f0;
    }

    public boolean isReady(int modelVersion) {
        if (this.storage.layerFunction.isFirst()) return true;
        return this.value.f1 == modelVersion;
    }
}
