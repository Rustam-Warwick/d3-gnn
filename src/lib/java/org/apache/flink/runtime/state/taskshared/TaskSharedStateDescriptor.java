package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.function.BiFunction;

/**
 * General State Descriptor for {@link TaskSharedState}
 * Value hold in this state is the same as the state itself
 * {@link org.apache.flink.api.common.state.StateDescriptor.Type} UNKNOWN is used to disallow such states being registered in normal state
 * <strong> Accepts a supplier for actually creating the state objects </strong>
 */
public class TaskSharedStateDescriptor<S extends TaskSharedState,V> extends StateDescriptor<S, V> {

    protected final BiFunction<TaskSharedStateDescriptor, TaskSharedKeyedStateBackend, S> stateSupplier;

    public TaskSharedStateDescriptor(String name, TypeInformation<V> typeInfo, BiFunction<TaskSharedStateDescriptor, TaskSharedKeyedStateBackend, S> stateSupplier) {
        super(name, typeInfo, null);
        this.stateSupplier = stateSupplier;
    }

    public BiFunction<TaskSharedStateDescriptor, TaskSharedKeyedStateBackend, S> getStateSupplier() {
        return stateSupplier;
    }

    @Override
    public Type getType() {
        return Type.UNKNOWN;
    }

}
