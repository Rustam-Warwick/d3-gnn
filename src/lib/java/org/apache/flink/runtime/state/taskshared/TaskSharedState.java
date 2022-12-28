package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.api.common.state.State;

/**
 * Base class for all task-shared state used in {@link TaskSharedKeyedStateBackend}
 */
abstract public class TaskSharedState implements State {

    public TaskSharedState(TaskSharedStateDescriptor<? extends TaskSharedState, ?> descriptor, TaskSharedKeyedStateBackend<?> backend){

    }
}
