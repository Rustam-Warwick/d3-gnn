package features;

import elements.Feature;
import elements.iterations.RemoteFunction;

import java.util.List;

public class Set<T> extends Feature<List<T>, List<T>> {
    public Set() {
        super();
    }

    public Set(Set<T> st, boolean deepCopy) {
        super(st, deepCopy);
    }

    public Set(List<T> value, boolean halo) {
        super(value, halo, (short) -1);
    }

    @Override
    public Set<T> copy() {
        return new Set<>(this, false);
    }

    @Override
    public Set<T> deepCopy() {
        return new Set<>(this, true);
    }

    @RemoteFunction
    public void add(T element) {
        if (this.value.contains(element)) return;
        this.value.add(element);
    }

    @RemoteFunction
    public void remove(T element) {
        if (!this.value.contains(element)) return;
        this.value.remove(element);
    }


    @RemoteFunction
    public void flush() {
        this.value.clear();
    }

    @Override
    public List<T> getValue() {
        return this.value;
    }

    @Override
    public boolean valuesEqual(List<T> v1, List<T> v2) {
        return false;
    }
}
