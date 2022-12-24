package elements.features;

import elements.Feature;
import elements.annotations.RemoteFunction;
import elements.enums.CopyContext;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * Special {@link Feature} representing the Replica parts of {@link elements.GraphElement}
 * Implements an array but works like a set
 */
public class Parts extends Feature<ShortArrayList, ShortArrayList> {

    public Parts() {
        super();
    }

    public Parts(String name, ShortArrayList value) {
        super(name, value);
    }

    public Parts(String name, ShortArrayList value, boolean halo) {
        super(name, value, halo);
    }

    public Parts(String name, ShortArrayList value, boolean halo, short master) {
        super(name, value, halo, master);
    }

    public Parts(Feature<ShortArrayList, ShortArrayList> feature, CopyContext context) {
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
    @RemoteFunction
    public void add(Short element) {
        if (this.value.contains(element)) return;
        this.value.add(element);
    }

    /**
     * {@link RemoteFunction} to remove a new part to the collection
     */
    @RemoteFunction
    public void remove(Short element) {
        if (!this.value.contains(element)) return;
        this.value.remove(element);
    }

    /**
     * {@link RemoteFunction} to remove all parts from a collection
     */
    @RemoteFunction
    public void flush() {
        this.value.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShortArrayList getValue() {
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
