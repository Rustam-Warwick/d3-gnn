package org.apache.flink.runtime.state.tmshared;

import org.apache.flink.api.common.state.State;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all task-shared state used in {@link TMSharedKeyedStateBackend}
 * This state does not actively scope to keys instead the state is shared and stored for all tasks
 * <ul>
 *     <li>Implementations should obviously be Thread-Safe</li>
 *     <li> Note that it is not a KVState but it can create a wrapper KVState if needed to register for publishable states</li>
 *     <li>For fault tolerance these states will be serialized for all the keys in every operator of TM</li>
 * </ul>
 */
abstract public class TMSharedState implements State {

    /**
     * Counter for each register() calls. Used to clear the state in correct time
     */
    protected AtomicInteger registrationCounter = new AtomicInteger(0);

    /**
     * Register sub-task to this shared state object.
     * Initialize variables and etc.
     */
    public void register(TMSharedKeyedStateBackend<?> TMSharedKeyedStateBackend) {
        registrationCounter.incrementAndGet();
    }

    /**
     * Deregister sub-task from this shared state object
     * Used for closing if the registrationCounter is back to 0
     *
     * @return if this was the last one and state should be removed
     */
    public boolean deregister(TMSharedKeyedStateBackend<?> TMSharedKeyedStateBackend) {
        if (registrationCounter.decrementAndGet() == 0) {
            clear();
            return true;
        }
        return false;
    }
}
