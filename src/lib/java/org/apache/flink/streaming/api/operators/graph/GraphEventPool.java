package org.apache.flink.streaming.api.operators.graph;

import elements.GraphEvent;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

/**
 * Event pool that stores the incoming flowing {@link GraphEvent} and optionally evicts them on merge
 * @implNote Only events streamed from withing the dataflow graph are entering this pool
 */
public class GraphEventPool implements Serializable {

    /**
     * Main operator to send the events on eviction
     */
    protected final OperatorEventHandler eventHandler;

    /**
     * Map of {@link GraphEvent}
     */
    protected final Map<GraphEvent, GraphEvent> events;

    /**
     * Cached {@link GraphRuntimeContext}
     */
    protected final GraphRuntimeContext graphRuntimeContext;

    public GraphEventPool(OperatorEventHandler streamOperator) {
        this.graphRuntimeContext = GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get();
        Preconditions.checkNotNull(graphRuntimeContext, "Cannot find graph runtime context");
        this.events = new Object2ObjectOpenHashMap<>(4);
        this.eventHandler = streamOperator;
    }

    /**
     * Add a new event object and merge
     */
    public void addEvent(GraphEvent event) {
        if (events.containsKey(event)) {
            events.get(event).merge(this, event);
        } else {
            events.put(event, event);
            event.merge(this, null);
        }
    }

    /**
     * Evict and handle event in the operator
     */
    public void evict(GraphEvent event) {
        events.remove(event);
        eventHandler.handleOperatorEvent(event);
    }

}
