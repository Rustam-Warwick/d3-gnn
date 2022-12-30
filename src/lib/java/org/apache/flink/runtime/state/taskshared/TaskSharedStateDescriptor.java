package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.function.Supplier;

/**
 * General State Descriptor for {@link TaskSharedState}
 * Value hold in this state is the same as the state itself
 * {@link org.apache.flink.api.common.state.StateDescriptor.Type} UNKNOWN is used to disallow such states being registered in normal state
 * <strong> Accepts a supplier for actually creating the task shared state objects</strong>
 */
public class TaskSharedStateDescriptor<S extends TaskSharedState, V> extends StateDescriptor<S, V> {

    /**
     * Supplier pattern for creating {@link TaskSharedState}
     */
    protected final Supplier<S> stateSupplier;

    public TaskSharedStateDescriptor(String name, TypeInformation<V> typeInfo, Supplier<S> stateSupplier) {
        super(name, typeInfo, null);
        this.stateSupplier = stateSupplier;
    }

    public Supplier<S> getStateSupplier() {
        return stateSupplier;
    }

    @Override
    public Type getType() {
        return Type.UNKNOWN;
    }

}
