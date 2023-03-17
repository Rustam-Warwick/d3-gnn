package org.apache.flink.runtime.state.taskshared;

/**
 * Task Shared State that stores a single value
 * @param <V> Type of the value stored
 */
public class TaskSharedValueState<V> extends TaskSharedState {

    final protected V value;

    public TaskSharedValueState(V value) {
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
