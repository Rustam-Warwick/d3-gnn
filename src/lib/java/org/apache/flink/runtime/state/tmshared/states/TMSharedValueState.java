package org.apache.flink.runtime.state.tmshared.states;

import org.apache.flink.runtime.state.tmshared.TMSharedState;

/**
 * Task Shared State that stores a single value
 *
 * @param <V> Type of the value stored
 */
public class TMSharedValueState<V> extends TMSharedState {

    final protected V value;

    public TMSharedValueState(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    @Override
    public void clear() {
        // pass
    }

}
