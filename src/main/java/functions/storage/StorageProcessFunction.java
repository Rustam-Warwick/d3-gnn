package functions.storage;

import elements.GraphElement;
import elements.GraphOp;
import elements.Rmi;
import elements.enums.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.BaseStorage;


/**
 * Interface that the storage and everything else interacts with
 * Real implementation should be tightly coupled with their respective operators
 */
public interface StorageProcessFunction extends RichFunction, CheckpointedFunction {

    Logger LOG = LoggerFactory.getLogger(StorageProcessFunction.class);

    /**
     * @return Attached storage engine
     */
    BaseStorage getStorage();

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
     * Message but also include the new timestmap as the timestmap of StreamRecord
     */
    void message(GraphOp op, MessageDirection direction, @NotNull Long timestamp);

    /**
     * Broadcast message in a specific direction
     */
    void broadcastMessage(GraphOp op, MessageDirection direction);

    /**
     * Broadcast message with the specified timestamp
     */
    void broadcastMessage(GraphOp op, MessageDirection direction, @NotNull Long timestamp);

    /**
     * Side outputs apart from those iterate, forward, backward messages
     */
    <OUT> void sideMessage(OUT op, OutputTag<OUT> outputTag);

    /**
     * Side outputs apart from those iterate, forward, backward messages with timestamp
     */
    <OUT> void sideMessage(OUT op, @NotNull OutputTag<OUT> outputTag, @NotNull Long timestamp);

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

    default void runForAllLocalParts(Runnable o) {
        getWrapperContext().runForAllKeys(o);
    }

    default void registerKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener) {
        getWrapperContext().registerKeyChangeListener(listener);
    }

    default void deRegisterKeyChangeListener(KeyedStateBackend.KeySelectionListener<Object> listener) {
        getWrapperContext().deRegisterKeyChangeListener(listener);
    }

    /**
     * @param value Process The Incoming Value
     */
    default void process(GraphOp value) {
        try {
            switch (value.op) {
                case COMMIT:
                    if (!getStorage().containsElement(value.element)) {
                        value.element.create();
                    } else {
                        getStorage().getElement(value.element).update(value.element);
                    }
                    break;
                case SYNC_REQUEST:
                    if (!getStorage().containsElement(value.element.getId(), value.element.getType())) {
                        // This can only occur if master is not here yet
                        GraphElement el = getStorage().getDummyElement(value.element.getId(), value.element.getType());
                        el.create();
                        el.syncRequest(value.element);
                    } else {
                        GraphElement el = getStorage().getElement(value.element.getId(), value.element.getType());
                        el.syncRequest(value.element);
                    }
                    break;
                case SYNC:
                    GraphElement el = getStorage().getElement(value.element);
                    el.sync(value.element);
                    break;
                case RMI:
                    GraphElement rpcElement = getStorage().getElement(value.element.getId(), value.element.getType());
                    Rmi rmi = (Rmi) value.element;
                    Rmi.execute(rpcElement, rmi.methodName, rmi.args);
                    break;
                case OPERATOR_EVENT:
                    getStorage().onOperatorEvent(value.operatorEvent);
                    break;
            }
        } catch (Exception | Error e) {
            LOG.error(ExceptionUtils.stringifyException(e), value);
        }
    }

}


