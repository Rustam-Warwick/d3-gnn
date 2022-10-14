package ai.djl.ndarray;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class GradientCollector<T> extends LinkedHashMap<T, NDArray> implements MayContainNDArray {
    public final transient boolean delayManagers;

    public GradientCollector(int initialCapacity, float loadFactor, boolean delayManagers) {
        super(initialCapacity, loadFactor);
        this.delayManagers = delayManagers;
    }

    public GradientCollector(int initialCapacity, boolean delayManagers) {
        super(initialCapacity);
        this.delayManagers = delayManagers;
    }

    public GradientCollector(boolean delayManagers) {
        this.delayManagers = delayManagers;
    }

    public GradientCollector(Map<? extends T, ? extends NDArray> m, boolean delayManagers) {
        super(m);
        this.delayManagers = delayManagers;
    }

    public GradientCollector(int initialCapacity, float loadFactor, boolean accessOrder, boolean delayManagers) {
        super(initialCapacity, loadFactor, accessOrder);
        this.delayManagers = delayManagers;
    }

    @Override
    public void putAll(Map<? extends T, ? extends NDArray> m) {
        m.forEach((key, value) -> {
            put(key, value);
        });
    }

    public NDArray put(T key, NDArray value) {
        return compute(key, (localKey, localValue) -> {
            if (localValue == null) {
                if(delayManagers) value.postpone();
                return value;
            } else {
                NDArray newValue = localValue.add(value);
                if(delayManagers){
                    newValue.postpone();
                    localValue.prepone();
                }
                return newValue;
            }
        });
    }

    //    public void put(T key, NDArray value){
//
//    }
    public void clear() {
        if(delayManagers) values().forEach(NDArray::prepone);
        super.clear();
    }
    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        values().forEach(operation);
    }
}
