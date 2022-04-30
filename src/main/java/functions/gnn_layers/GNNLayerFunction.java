package functions.gnn_layers;

import elements.GraphElement;
import elements.GraphOp;
import elements.ReplicaState;
import iterations.Rmi;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.util.List;
import java.util.Objects;

/**
 * Function that the storage and everything else interacts with
 * Real implementation should be tightly coupled with their respective operators
 */
public interface GNNLayerFunction extends RichFunction {
    /**
     * Get Current Part that is being processed
     *
     * @return
     */
    short getCurrentPart();

    /**
     * Get horizontal position of this GNN Layer
     *
     * @return
     */
    short getPosition();

    /**
     * Get depth of the GNN Layers
     *
     * @return
     */
    short getNumLayers();

    /**
     * Send message. Should handle BACKWARD, FORWARD and ITERATE Messages separately
     *
     * @param op
     */
    void message(GraphOp op);


    /**
     * Side outputs
     */
    void sideMessage(GraphOp op, OutputTag<GraphOp> outputTag);

    /**
     * Storage Attached to this GNNLayer
     *
     * @return
     */
    BaseStorage getStorage();

    /**
     * Get the timerservice
     *
     * @return the TimerService
     */
    TimerService getTimerService();

    /**
     * Callback on watermark extra callback to be registered for plugins
     * @param mark received watermark
     */
    void onWatermark(Watermark mark);

    /**
     * Is this the first GNN-layer
     *
     * @return
     */
    default boolean isFirst() {
        return getPosition() == 1;
    }

    /**
     * Is this the last or output GNN-layer
     *
     * @return
     */
    default boolean isLast() {
        return getPosition() >= getNumLayers();
    }

    /**
     * Process incoming value
     *
     * @param value
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
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
        } finally {
            getStorage().manager.clean();
        }
    }

}
