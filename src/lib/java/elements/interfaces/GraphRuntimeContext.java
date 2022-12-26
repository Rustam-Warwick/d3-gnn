package elements.interfaces;

import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.util.List;

/**
 * Runtime Context with extra Graph Pipeline based operations and output functions
 * <p>
 * Essentially exposing a lot of {@link org.apache.flink.streaming.api.operators.StreamOperator} API is needed
 * to send messages from {@link elements.GraphElement} at various levels of Graph callbacks
 * Also acts as {@link GraphListener} for element deltas
 * </p>
 */
public abstract class GraphRuntimeContext implements RuntimeContext, GraphListener {

    /**
     * ThreadLocal variable holding the {@link GraphRuntimeContext} for {@link elements.GraphElement} to access
     * Need to populate this during the {@link java.lang.reflect.Constructor} of the implementation of this class
     */
    public static ThreadLocal<GraphRuntimeContext> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * Get the {@link BaseStorage}
     */
    abstract public BaseStorage getStorage();

    /**
     * Get the {@link Plugin} specified by the ID
     */
    abstract public Plugin getPlugin(String pluginId);

    /**
     * Send {@link GraphOp} forwards down the pipeline
     */
    abstract public void output(GraphOp op);

    /**
     * Send {@link GraphOp} to specific {@link OutputTag} with same type
     */
    abstract public void output(GraphOp op, OutputTag<GraphOp> tag);

    /**
     * Send <T> element to its output tag
     */
    abstract public <T> void output(T el, OutputTag<T> tag);

    /**
     * Broadcast {@link GraphOp} down the pipeline
     */
    abstract public void broadcast(GraphOp op);

    /**
     * Broadcast {@link GraphOp} to specific {@link OutputTag} with same type
     */
    abstract public void broadcast(GraphOp op, OutputTag<GraphOp> tag);

    /**
     * Broadcast {@link GraphOp} only to the selected parts down the pipeline
     */
    abstract public void broadcast(GraphOp op, List<Short> selectedPartsOnly);

    /**
     * Broadcast {@link GraphOp} but only to the selected parts
     */
    abstract public void broadcast(GraphOp op, OutputTag<GraphOp> tag, List<Short> selectedPartsOnly);

    /**
     * Run the {@link Runnable} in all parts of this Operator
     */
    abstract public void runForAllLocalParts(Runnable run);

    /**
     * Return the {@link TimerService}
     */
    abstract public TimerService getTimerService();

    /**
     * Get the position of this graph storage in the entire pipeline
     */
    abstract public short getPosition();

    /**
     * Get current part of this storage that is being processed
     */
    abstract public short getCurrentPart();

    /**
     * Timestamp of the element currently being processed
     */
    abstract public long currentTimestamp();

    /**
     * Is this Graph Storage the first in the pipeline
     */
    boolean isFirst() {
        return getPosition() <= 1;
    }
}
