package features;

import elements.Feature;
import elements.GraphElement;
import iterations.RemoteFunction;

import java.util.List;

public class Set<T> extends Feature<List<T>, List<T>> {
    public Set() {
    }

    public Set(List<T> value) {
        super(value);
    }

    public Set(List<T> value, boolean halo) {
        super(value, halo);
    }

    public Set(List<T> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public Set(String id, List<T> value) {
        super(id, value);
    }

    public Set(String id, List<T> value, boolean halo) {
        super(id, value, halo);
    }

    public Set(String id, List<T> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        Set<T> tmp = new Set<T>(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        Set<T> tmp = new Set<T>(this.id, this.value, this.halo, this.master);
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
