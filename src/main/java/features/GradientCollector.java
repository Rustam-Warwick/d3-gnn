package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;

import java.util.HashMap;

/**
 * Helper Feature for VertexTrainer
 */
public class GradientCollector<T> extends Feature<HashMap<T, NDArray>, HashMap<T, NDArray>> {
    public GradientCollector() {
    }

    public GradientCollector(Feature<HashMap<T, NDArray>, HashMap<T, NDArray>> f, boolean deepCopy) {
        super(f, deepCopy);
    }

    public GradientCollector(HashMap<T, NDArray> value) {
        super(value);
    }

    public GradientCollector(HashMap<T, NDArray> value, boolean halo, Short master) {
        super(value, halo, master);
    }

    public GradientCollector(String id, HashMap<T, NDArray> value) {
        super(id, value);
    }

    public GradientCollector(String id, HashMap<T, NDArray> value, boolean halo, Short master) {
        super(id, value, halo, master);
    }

    public void merge(HashMap<T, NDArray> incoming){
        if(incoming.isEmpty()) return;
        incoming.forEach((key, grad)->{
            this.value.compute(key, (localKey, localGrad)->{
               if(localGrad == null){
                   grad.detach();
                   return grad;
               }
               else{
                   localGrad.addi(grad);
                   return localGrad;
               }
            });
        });
        storage.updateFeature(this);
    }

    public void clean(){
        if(value.isEmpty()) return;
        value.clear();
        storage.updateFeature(this);
    }
}
