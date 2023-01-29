package org.apache.flink.runtime.state.taskshared;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.state.State;

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
     * Group ID to index. Index represents a logical index(order) of registration for this group ID
     */
    protected final Int2IntOpenHashMap groupIdToIndex = new Int2IntOpenHashMap();

    /**
     * Counter for each register() calls. Used to populate group-to-index map
     */
    protected int registrationCounter = 0;

    /**
     * Register sub-task to this shared state object
     */
    public synchronized void register(TaskSharedKeyedStateBackend<?> taskSharedStateBackend) {
        for (int i = taskSharedStateBackend.getKeyGroupRange().getStartKeyGroup(); i <= taskSharedStateBackend.getKeyGroupRange().getEndKeyGroup(); i++) {
            groupIdToIndex.put(i, registrationCounter);
        }
        registrationCounter++;
    }



}
