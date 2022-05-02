package features;

import elements.Feature;
import iterations.RemoteFunction;

import java.util.List;

public class Set<T> extends Feature<List<T>, List<T>> {
    public Set() {
    }

    public Set(List<T> value, boolean halo) {
        super(value, halo, (short) -1);
    }

    public Set(String id, List<T> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public Set<T> copy() {
        Set<T> tmp = new Set<T>(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Set<T> deepCopy() {
        Set<T> tmp = new Set<T>(this.id, this.value, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
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
