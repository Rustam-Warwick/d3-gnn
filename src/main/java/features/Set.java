package features;

import elements.Feature;
import elements.GraphElement;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class Set<T> extends Feature<List<T>> {
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
        Set<T> tmp = new Set<T>(this.id, (List<T>) this.value, this.halo, this.master);
        tmp.setPartId(this.getPartId());
        tmp.setStorage(this.storage);
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        List<T> valueCopy = ((List<T>)this.value);
        Set<T> tmp = new Set<T>(this.id, valueCopy, this.halo, this.master);
        tmp.setPartId(this.getPartId());
        tmp.setStorage(this.storage);
        tmp.features.putAll(this.features);
        return tmp;
    }

    public void add(T element){
        if(((List<T>) this.value).contains(element))return;
        ((List<T>) this.value).add(element);
        
    }

    @Override
    public List<T> getValue() {
        return (List<T>) this.value;
    }

    @Override
    public boolean valuesEqual(Object v1, Object v2) {
        return false;
    }
}
