package org.apache.flink.streaming.api.operators.graph.interfaces;

import elements.GraphOp;
import elements.Plugin;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.taskshared.TaskSharedKeyedStateBackend;
import org.apache.flink.runtime.state.taskshared.TaskSharedState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.util.List;

/**
 * Runtime Context saved in ThreadLocal state with extra Graph Pipeline based operations and output functions
 * <p>
 * Essentially exposing a lot of {@link org.apache.flink.streaming.api.operators.StreamOperator} API is needed
 * to send messages from {@link elements.GraphElement} at various levels of Graph callbacks
 * Also acts as {@link GraphListener} for storage updates
 * </p>
 */
public abstract class GraphRuntimeContext implements RuntimeContext, GraphListener {

    /**
     * ThreadLocal variable holding the {@link GraphRuntimeContext} for {@link elements.GraphElement} to access
     * Need to populate this during the {@link java.lang.reflect.Constructor} of the implementation of this class
     */
    public static ThreadLocal<GraphRuntimeContext> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * This operator parts list
     */
    protected ShortList thisOperatorParts = new ShortArrayList();

    public GraphRuntimeContext() {
        PartNumber tmpPartNumber = PartNumber.of((short) 0);
        for (short i = 0; i < getMaxNumberOfParallelSubtasks(); i++) {
            tmpPartNumber.partId = i;
            int op = KeyGroupRangeAssignment.assignKeyToParallelOperator(tmpPartNumber, getMaxNumberOfParallelSubtasks(), getNumberOfParallelSubtasks());
            if (op == getIndexOfThisSubtask()) thisOperatorParts.add(i);
        }
        CONTEXT_THREAD_LOCAL.set(this);
    }

    /**
     * Get the {@link BaseStorage}
     */
    abstract public BaseStorage.GraphView getStorage();

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
     * Broadcast {@link GraphOp} down the pipeline to connected edges
     */
    abstract public void broadcastAll(GraphOp op);

    /**
     * Broadcast {@link GraphOp} down the forward pipeline
     *
     * @implNote Broadcast GraphOps should have messageCommunication as broadcast otherwise key error will occur
     */
    abstract public void broadcast(GraphOp op);

    /**
     * Broadcast {@link GraphOp} to specific {@link OutputTag} with same type
     *
     * @implNote Broadcast GraphOps should have messageCommunication as broadcast otherwise key error will occur
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
     * Get the number of output channels in the forward layer
     *
     * @return
     */
    abstract public int getNumOfOutChannels();


    /**
     * Get the number of output channels in the {@link OutputTag}
     *
     * @return
     */
    abstract public int getNumOfOutChannels(OutputTag<GraphOp> tag);

    /**
     * Return {@link TaskSharedKeyedStateBackend}
     */
    abstract public TaskSharedKeyedStateBackend<PartNumber> getKeyedStateBackend();

    /**
     * Get Task Shared State from {@link org.apache.flink.runtime.state.taskshared.TaskSharedStateBackend}
     */
    abstract public <S extends TaskSharedState> S getTaskSharedState(TaskSharedStateDescriptor<S, ?> taskSharedStateDescriptor);

    /**
     * Run the {@link Runnable} in all parts of this Operator
     */
    abstract public void runForAllLocalParts(Runnable run);

    /**
     * Return the {@link TimerService}
     */
    abstract public TimerService getTimerService();

    /**
     * Get all input gates
     */
    abstract public IndexedInputGate[] getInputGates();

    /**
     * Get the position of this graph storage in the entire pipeline
     */
    abstract public short getPosition();

    /**
     * Get the number of layers in the pipeline
     */
    abstract public short getLayers();

    /**
     * Get current part of this storage that is being processed
     */
    abstract public short getCurrentPart();

    /**
     * Timestamp of the element currently being processed
     */
    abstract public long currentTimestamp();

    /**
     * Return {@link OperatorEventGateway}
     */
    abstract public OperatorEventGateway getOperatorEventGateway();

    /**
     * Send {@link OperatorEvent} to Operator Coordinator
     */
    public final void sendOperatorEvent(OperatorEvent event) {
        getOperatorEventGateway().sendEventToCoordinator(event);
    }

    /**
     * Gets the list of parts mapped to this operator
     */
    public final ShortList getThisOperatorParts() {
        return thisOperatorParts;
    }

    /**
     * Is this operator the last one in the pipeline
     */
    public final boolean isLast(){
        return getPosition() == getLayers();
    }

    /**
     * Is this Graph Storage the first in the pipeline: First is either SPLITTER and First Storage layer
     */
    public final boolean isFirst() {
        return getPosition() == 1;
    }

    /**
     * Is this Graph Splitter operator
     */
    public final boolean isSplitter(){
        return getPosition() == 0;
    }
}
