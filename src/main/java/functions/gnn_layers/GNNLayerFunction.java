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
 * Function that the storage and everything else interacts with
 * Real implementation should be tightly coupled with their respective operators
 */
public interface GNNLayerFunction extends RichFunction {
    /**
     * @return Current Part that is being processed
     */
    short getCurrentPart();

    void setCurrentPart(short part);

    /**
     * @return Horizontal Position
     */
    short getPosition();

    void setPosition(short position);

    /**
     * @return Number of GNN Layers
     */
    short getNumLayers();

    void setNumLayers(short numLayers);

    /**
     * BaseWrapper Context for sending broadCastElements
     */
    BaseWrapperOperator.Context getWrapperContext();

    void setWrapperContext(BaseWrapperOperator.Context context);

    /**
     * Send message. Should handle BACKWARD, FORWARD and ITERATE Messages separately
     *
     * @param op GraphOp to be sent
     */
    void message(GraphOp op, MessageDirection direction);

    /**
     * Side outputs
     */
    void sideMessage(GraphOp op, OutputTag<GraphOp> outputTag);

    /**
     * @return Storage
     */
    BaseStorage getStorage();

    void setStorage(BaseStorage storage);

    /**
     * @return TimerService fro managing timers and watermarks and stuff like that
     */
    TimerService getTimerService();

    /**
     * Get the current timestamp being processed
     */
    long currentTimestamp();

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
            getStorage().manager.clean();
        }
    }

}


