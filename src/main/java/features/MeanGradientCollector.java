package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

/**
 * A Feature that helps collecting gradients for ModelServer and etc.
 */
public class MeanGradientCollector<T> extends Feature<Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>>, HashMap<T, NDArray>> {
    public MeanGradientCollector() {
    }

    public MeanGradientCollector(Feature<Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>>, HashMap<T, NDArray>> f, boolean deepCopy) {
        super(f, deepCopy);
    }

    public MeanGradientCollector(Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>> value) {
        super(value);
    }

    public MeanGradientCollector(Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>> value, boolean halo, Short master) {
        super(value, halo, master);
    }

    public MeanGradientCollector(String id, Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>> value) {
        super(id, value);
    }

    public MeanGradientCollector(String id, Tuple2<HashMap<T, NDArray>, HashMap<T, Integer>> value, boolean halo, Short master) {
        super(id, value, halo, master);
    }

    @Override
    public HashMap<T, NDArray> getValue() {
        return this.value.f0;
    }

    public void merge(HashMap<T, NDArray> incoming) {
        if (incoming.isEmpty()) return;
        incoming.forEach((key, grad) -> {
            if (!grad.isValid()) return;
            this.value.f0.compute(key, (localKey, localGrad) -> {
                if (localGrad == null) {
                    grad.detach();
                    value.f1.put(localKey, 1);
                    return grad;
                } else {
                    value.f1.merge(localKey, 1, Integer::sum);
                    return localGrad.add(grad);
                }
            });
        });
        storage.updateFeature(this);
    }

    public void clean() {
        value.f1.clear();
        value.f0.clear();
        storage.updateFeature(this);
    }
}
