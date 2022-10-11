package helpers;

import ai.djl.ndarray.NDArray;

import java.util.HashMap;
import java.util.Map;

public class GradientCollector<T> extends HashMap<T, NDArray> {
    public GradientCollector(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public GradientCollector(int initialCapacity) {
        super(initialCapacity);
    }

    public GradientCollector() {
    }

    public GradientCollector(Map<? extends T, ? extends NDArray> m) {
        super(m);
    }

    public NDArray putPostpone(T key, NDArray value){
        value.postpone();
        return super.put(key, value);
    }

    public void clearPrepone(){
        values().forEach(NDArray::prepone);
        super.clear();
    }

    public void merge(Map<T, NDArray> incoming){
        if (incoming.isEmpty()) return;
        incoming.forEach((key, grad) -> {
            if (!grad.isValid()) return;
            compute(key, (localKey, localGrad) -> {
                if (localGrad == null) {
                    grad.postpone();
                    return grad;
                } else {
                    localGrad.addi(grad);
                    return localGrad;
                }
            });
        });
    }
}
