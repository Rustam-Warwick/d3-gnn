package ai.djl.ndarray;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class NDArrayCollector<T> extends LinkedHashMap<T, NDArray> implements MayContainNDArray {
    public final transient boolean delayManagers;

    public NDArrayCollector(int initialCapacity, float loadFactor, boolean delayManagers) {
        super(initialCapacity, loadFactor);
        this.delayManagers = delayManagers;
    }

    public NDArrayCollector(int initialCapacity, boolean delayManagers) {
        super(initialCapacity);
        this.delayManagers = delayManagers;
    }

    public NDArrayCollector(boolean delayManagers) {
        this.delayManagers = delayManagers;
    }

    public NDArrayCollector(Map<? extends T, ? extends NDArray> m, boolean delayManagers) {
        super(m);
        this.delayManagers = delayManagers;
    }

    public NDArrayCollector(int initialCapacity, float loadFactor, boolean accessOrder, boolean delayManagers) {
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
        if (!containsKey(key) && delayManagers) value.postpone();
        merge(key, value, NDArray::addi);
//        return compute(key, (localKey, localValue) -> {
//            if (localValue == null) {
//                if(delayManagers) value.postpone();
//                return value;
//            } else {
//                NDArray newValue = localValue.add(value);
//                if(delayManagers){
//                    newValue.postpone();
//                    localValue.prepone();
//                }
//                return newValue;
//            }
//        });
        return get(key);
    }

    //    public void put(T key, NDArray value){
//
//    }
    public void clear() {
        if (delayManagers) values().forEach(NDArray::prepone);
        super.clear();
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        values().forEach(operation);
    }
}
