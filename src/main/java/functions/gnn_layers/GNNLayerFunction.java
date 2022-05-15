package functions.gnn_layers;

import elements.GraphElement;
import elements.GraphOp;
import elements.ReplicaState;
import iterations.MessageDirection;
import iterations.Rmi;
import operators.BaseWrapperOperator;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.util.Objects;

/**
 * Interface that the storage and everything else interacts with
 * Real implementation should be tightly coupled with their respective operators
 */
public interface GNNLayerFunction extends RichFunction {
    // ---------------> Externally set Features, Variables

    /**
     * @return Current Part that is being processed
     */
    short getCurrentPart();

    /**
     * Set the current part that is being processed
     *
     * @param part Part
     */
    void setCurrentPart(short part);

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
    long currentTimestamp();

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

    // ----------------> Derived methods

    /**
     * @return Is this the first GNN Layer
     */
    default boolean isFirst() {
        return getPosition() == 1;
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

    /**
     * @param value Process The Incoming Value
     */
    default void process(GraphOp value) {
        try {
            switch (value.op) {
                case COMMIT:
                    GraphElement thisElement = getStorage().getElement(value.element);
                    if (Objects.isNull(thisElement)) {
                        value.element.setStorage(getStorage());
                        value.element.create();
                    } else {
                        thisElement.update(value.element);
                    }
                    break;
                case SYNC:
                    GraphElement el = this.getStorage().getElement(value.element);
                    if (Objects.isNull(el)) {
                        el = value.element.copy();
                        el.setStorage(getStorage());
                        if (el.state() == ReplicaState.MASTER) {
                            // Replicas should not be created by master since they are the first parties sending sync messages
                            el.create();
                            el.sync(value.element);
                        }
                    } else {
                        el.sync(value.element);
                    }
                    break;
                case RMI:
                    GraphElement rpcElement = this.getStorage().getElement(value.element.getId(), value.element.elementType());
                    Rmi.execute(rpcElement, (Rmi) value.element);
                    break;
                case WATERMARK:
                    getStorage().onWatermark(value.getTimestamp());
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
        } finally {

        }
    }

}


