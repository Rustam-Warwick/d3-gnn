package features;

import elements.Feature;
import elements.GraphElement;
import iterations.RemoteFunction;

import java.util.List;

public class TString extends Feature<String, String> {
    public TString() {
    }

    public TString(String value) {
        super(value);
    }

    public TString(String value, boolean halo) {
        super(value, halo);
    }

    public TString(String value, boolean halo, short master) {
        super(value, halo, master);
    }

    public TString(java.lang.String id, String value) {
        super(id, value);
    }

    public TString(java.lang.String id, String value, boolean halo) {
        super(id, value, halo);
    }

    public TString(java.lang.String id, String value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        TString tmp = new TString(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        TString tmp = new TString(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }


    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public boolean valuesEqual(String v1, String v2) {
        return v1.equals(v2);
    }
}
