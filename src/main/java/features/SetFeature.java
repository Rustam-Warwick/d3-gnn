package features;

import elements.Feature;

import java.util.HashSet;

public class SetFeature<T> extends Feature<HashSet<T>> {

    public SetFeature(String id, HashSet<T> value) {
        super(id, value);
    }

    public SetFeature(String id) {
        super(id, new HashSet<T>());
    }

    public SetFeature(String id, short part_id) {
        super(id, new HashSet<T>(), part_id);
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
