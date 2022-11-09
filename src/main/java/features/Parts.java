package features;

import elements.Feature;
import elements.annotations.RemoteFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

public class Parts extends Feature<List<Short>, List<Short>> {
    public Parts() {
        super();
    }

    public Parts(Parts st, boolean deepCopy) {
        super(st, deepCopy);
    }

    public Parts(List<Short> value, boolean halo) {
        super(value, halo, (short) -1);
    }

    public Parts(String id, List<Short> val, boolean halo, short master) {
        super(id, val, halo, master);
    }

    @Override
    public Parts copy() {
        return new Parts(this, false);
    }

    @Override
    public Parts deepCopy() {
        return new Parts(this, true);
    }

    @RemoteFunction
    public void add(Short element) {
        if (this.value.contains(element)) return;
        this.value.add(element);
    }

    @RemoteFunction
    public void remove(Short element) {
        if (!this.value.contains(element)) return;
        this.value.remove(element);
    }


    @RemoteFunction
    public void flush() {
        this.value.clear();
    }

    @Override
    public List<Short> getValue() {
        return this.value;
    }

    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return Types.LIST(Types.SHORT);
    }
}
