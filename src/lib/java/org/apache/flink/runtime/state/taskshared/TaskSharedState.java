package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.api.common.state.State;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all task-shared state used in {@link TaskSharedKeyedStateBackend}
 * This state does not actively scope to keys instead the state is shared and stored for all tasks
 * Implementations should obviously be Thread-Safe
 * <p>
 * Note that it is not a KVState but it can create a wrapper KVState if needed to register for publishable states
 * </p>
 */
abstract public class TaskSharedState implements State {

    /**
     * Counter for each register() calls. Used to clear the state in correct time
     */
    protected AtomicInteger registrationCounter = new AtomicInteger(0);

    /**
     * Register sub-task to this shared state object
     */
    public void register(TaskSharedKeyedStateBackend<?> taskSharedKeyedStateBackend) {
        registrationCounter.incrementAndGet();
    }

    /**
     * Deregister sub-task from this shared state object
     * Used for closing
     *
     * @return if this was the last one and state should be removed
     */
    public boolean deregister(TaskSharedKeyedStateBackend<?> taskSharedKeyedStateBackend) {
        if (registrationCounter.decrementAndGet() == 0) {
            clear();
            return true;
        }
        return false;
    }


}
