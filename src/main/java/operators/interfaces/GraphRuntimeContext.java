package operators.interfaces;

import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

/**
 * Runtime Context with extra Graph Pipeline based operations and output functions
 * <p>
 *     Essentially exposing a lot of {@link org.apache.flink.streaming.api.operators.StreamOperator} API is needed
 *     to send messages from {@link elements.GraphElement} at various levels
 * </p>
 */
public interface GraphRuntimeContext extends RuntimeContext {

    /**
     * ThreadLocal variable holding the {@link GraphRuntimeContext}
     */
    ThreadLocal<GraphRuntimeContext> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * Get the current {@link GraphRuntimeContext}
     */
    static GraphRuntimeContext getContext(){
        return CONTEXT_THREAD_LOCAL.get();
    }

    /**
     * Get the {@link BaseStorage}
     */
    BaseStorage getStorage();

    /**
     * Get the {@link Plugin} specified by the ID
     */
    Plugin getPlugin(String pluginId);

    /**
     * Send {@link GraphOp} forwards down the pipeline
     */
    void message(GraphOp op);

    /**
     * Send {@link GraphOp} to specific {@link OutputTag} with same type
     */
    void message(GraphOp op, OutputTag<GraphOp> tag);

    /**
     * Broadcast {@link GraphOp} down the pipeline
     */
    void broadcastMessage(GraphOp op);

    /**
     * Broadcast {@link GraphOp} to specific {@link OutputTag} with same type
     */
    void broadcastMessage(GraphOp op, OutputTag<GraphOp> tag);

    /**
     * Broadcast {@link GraphOp} but only to the selected parts
     */
    void broadcastMessage(GraphOp op, OutputTag<GraphOp> tag, short... selectedPartsOnly);

    /**
     * Run the {@link Runnable} in all parts of this Operator
     */
    void runForAllLocalParts(Runnable run);

    /**
     * Return the {@link TimerService}
     */
    TimerService getTimerService();

    /**
     * Get the position of this graph storage in the entire pipeline
     */
    int getPosition();

    /**
     * Get current part of this storage that is being processed
     */
    short getCurrentPart();

    /**
     * Timestamp of the element currently being processed
     */
    long currentTimestamp();

    /**
     * Is this Graph Storage the first in the pipeline
     */
    default boolean isFirst(){
        return getPosition() <= 1;
    }

}
