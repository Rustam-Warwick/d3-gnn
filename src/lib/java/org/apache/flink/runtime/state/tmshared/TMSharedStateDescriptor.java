package org.apache.flink.runtime.state.tmshared;

import org.apache.flink.api.common.state.StateDescriptor;

import java.util.function.Supplier;

/**
 * General State Descriptor for {@link TMSharedState}
 * Value hold in this state is the same as the state itself
 * {@link org.apache.flink.api.common.state.StateDescriptor.Type} UNKNOWN is used to disallow such states being registered in normal state
 * <strong> Accepts a supplier for actually creating the task shared state objects</strong>
 */
public class TMSharedStateDescriptor<S extends TMSharedState, V> extends StateDescriptor<S, V> {

    /**
     * Supplier pattern for creating {@link TMSharedState}
     */
    protected final Supplier<S> stateSupplier;

    public TMSharedStateDescriptor(String name, Class<V> type, Supplier<S> stateSupplier) {
        super(name, type, null);
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
