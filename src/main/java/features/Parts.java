package features;

import elements.Feature;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

/**
 * Special {@link Feature} representing the Replica parts of {@link elements.GraphElement}
 * Implements an array but works like a set
 */
public class Parts extends Feature<List<Short>, List<Short>> {
    public Parts() {
    }

    public Parts(String name, List<Short> value) {
        super(name, value);
    }

    public Parts(String name, List<Short> value, boolean halo) {
        super(name, value, halo);
    }

    public Parts(String name, List<Short> value, boolean halo, short master) {
        super(name, value, halo, master);
    }

    public Parts(Feature<List<Short>, List<Short>> feature, CopyContext context) {
        super(feature, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Parts copy(CopyContext context) {
        return new Parts(this, context);
    }

    /**
     * {@link RemoteFunction} to add a new part to the collection
     */
    @RemoteFunction(triggerCallbacks = false)
    public void add(Short element) {
        if (this.value.contains(element)) return;
        this.value.add(element);
    }

    /**
     * {@link RemoteFunction} to remove a new part to the collection
     */
    @RemoteFunction(triggerCallbacks = false)
    public void remove(Short element) {
        if (!this.value.contains(element)) return;
        this.value.remove(element);
    }

    /**
     * {@link RemoteFunction} to remove all parts from a collection
     */
    @RemoteFunction(triggerCallbacks = false)
    public void flush() {
        this.value.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Short> getValue() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeInformation<?> getValueTypeInfo() {
        return Types.LIST(Types.SHORT);
    }
}
