package features;

import elements.Feature;

import java.util.HashSet;

public class Set<T> extends Feature<HashSet<T>> {
    public Set(HashSet<T> value) {
        super(value);
    }

    public Set(HashSet<T> value, boolean halo) {
        super(value, halo);
    }

    public Set(String id, HashSet<T> value) {
        super(id, value);
    }

    public Set(String id, HashSet<T> value, boolean halo) {
        super(id, value, halo);
    }

    @Override
    public HashSet<T> getValue() {
        return (HashSet<T>) this.value;
    }

    @Override
    public boolean valuesEqual(Object v1, Object v2) {
        return false;
    }
}
