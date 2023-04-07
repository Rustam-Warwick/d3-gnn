package elements;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.GraphEventPool;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for operator events that flow in the GNN pipeline acting as synchronization barriers or encompassing other logic
 * <p>
 * Life cycle of these events are tightly coupled with {@link GraphEventPool}
 * Incoming {@link GraphEvent} is first added to the pool
 * Then after addition the {@code merge} function is called with no arguments
 * If same {@link GraphEvent} arrives second time instead {@code merge} function is called with the arrived event
 * At every merge call the event can decide to evict itself from the {@link GraphEventPool}
 * which means that it is ready to be processed by the underlying operator
 * </p>
 */
abstract public class GraphEvent implements OperatorEvent {

    /**
     * Everytime such event arrives we call the merge method to merge old event with new one
     * At some point the event will decide to evict itself by calling {@code pool.evict}
     */
    public void merge(GraphEventPool pool, @Nullable GraphEvent incoming) {
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj.getClass().equals(getClass());
    }
}
