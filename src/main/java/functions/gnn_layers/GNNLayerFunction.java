package functions.gnn_layers;

import elements.GraphElement;
import elements.GraphOp;
import elements.ReplicaState;
import elements.iterations.MessageDirection;
import elements.iterations.Rmi;
import operators.BaseWrapperOperator;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import javax.annotation.Nullable;

/**
 * Interface that the storage and everything else interacts with
 * Real implementation should be tightly coupled with their respective operators
 */
public interface GNNLayerFunction extends RichFunction, CheckpointedFunction {
    // ---------------> Externally set Features, Variables

    /**
     * @return Attached storage engine
     */
    BaseStorage getStorage();

    /**
     * Set the storage engine
     */
    void setStorage(BaseStorage storage);

    /**
     * BaseWrapper Context for doing higher-order stuff
     */
    BaseWrapperOperator<?>.Context getWrapperContext();

    /**
     * Set the base wrapper context
     */
    void setWrapperContext(BaseWrapperOperator<?>.Context context);

    /**
     * @return TimerService for managing timers and watermarks and stuff like that
     */
    TimerService getTimerService();

    /**
     * Get the current timestamp that being processed
     */
    Long currentTimestamp();

    // ----------------> Communication primitives

    /**
     * Send message. Should handle BACKWARD, FORWARD and ITERATE Messages separately
     *
     * @param op GraphOp to be sent
     */
    void message(GraphOp op, MessageDirection direction);

    /**
     * Broadcast message in a specific direction
     */
    void broadcastMessage(GraphOp op, MessageDirection direction);

    /**
     * Side outputs apart from those iterate, forward, backward messages
     */
    <OUT> void sideMessage(OUT op, OutputTag<OUT> outputTag);

    /**
     * Broadcast message to a specific side output
     *
     * @param op        Operation
     * @param outputTag OutputTag to broadcast to
     * @param <OUT>     Type of the message to be broadcasted
     */
    <OUT> void sideBroadcastMessage(OUT op, OutputTag<OUT> outputTag);

    /**
     * Send some event to the operator coordinator
     *
     * @param operatorEvent OperatorEvent
     */
    default void operatorEventMessage(OperatorEvent operatorEvent) {
        getWrapperContext().sendOperatorEvent(operatorEvent);
    }

    // ----------------> Derived methods

    @Override
    default void snapshotState(FunctionSnapshotContext context) throws Exception {
        getStorage().snapshotState(context);
    }

    @Override
    default void initializeState(FunctionInitializationContext context) throws Exception {
        getStorage().initializeState(context);
    }

    /**
     * Get the current part of this operator
     */
    default Short getCurrentPart() {
        return getWrapperContext().currentPart();
    }

    /**
     * @return Is this the first GNN Layer
     */
    default boolean isFirst() {
        return getPosition() <= 1;
    }

    /**
     * @return Is this the last GNN Layer
     */
    default boolean isLast() {
        return getPosition() >= getNumLayers();
    }

    default short getPosition() {
        return getWrapperContext().getPosition();
    }

    default short getNumLayers() {
        return getWrapperContext().getNumLayers();
    }

    default int getNumberOfOutChannels(@Nullable OutputTag<?> tag) {
        return getWrapperContext().getNumberOfOutChannels(tag);
    }

    /**
     * @param value Process The Incoming Value
     */
    default void process(GraphOp value) {
        try {
            if (value.element != null) value.element.setStorage(getStorage());
            switch (value.op) {
                case COMMIT:
                    if (!getStorage().containsElement(value.element)) {
                        value.element.create();

                    } else {
                        GraphElement thisElement = getStorage().getElement(value.element);
                        thisElement.update(value.element);
                    }
                    break;
                case SYNC:
                    if (!getStorage().containsElement(value.element)) {
                        GraphElement el = value.element.copy();
                        el.setStorage(getStorage());
                        el.setPartId(getCurrentPart());
                        if (el.state() == ReplicaState.MASTER) {
                            // Replicas should not be created by master since they are the first parties sending sync messages
                            el.create();
                            el.sync(value.element);
                        }
                    } else {
                        GraphElement el = this.getStorage().getElement(value.element);
                        el.sync(value.element);
                    }
                    break;
                case RMI:
                    GraphElement rpcElement = this.getStorage().getElement(value.element.getId(), value.element.elementType());
                    Rmi.execute(rpcElement, (Rmi) value.element);
                    break;
                case OPERATOR_EVENT:
                    getStorage().onOperatorEvent(value.getOperatorEvent());
                    break;
            }
        } catch (Exception | Error e) {
            System.out.println(value);
            e.printStackTrace();
        } finally {
            getStorage().getPlugins().forEach(GraphElement::clearFeatures); // Clear the features since it may come later
        }
    }

}


