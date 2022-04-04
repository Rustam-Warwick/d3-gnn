package functions;

import elements.*;
import iterations.IterationType;
import iterations.Rmi;
import org.apache.flink.api.common.functions.RichFunction;
import storage.BaseStorage;

import java.util.List;
import java.util.Objects;

public interface GNNLayerFunction extends RichFunction {
    /**
     * Get Current Part that is being processed
     *
     * @return
     */
    short getCurrentPart();

    /**
     * Get master part of this operator
     *
     * @return
     */
    short getMasterPart();

    /**
     * Get all parts hashed to this operator
     *
     * @return
     */
    List<Short> getThisParts();

    /**
     * Get master parts of other parallel operators
     *
     * @return
     */
    List<Short> getReplicaMasterParts();

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
    short getLayers();

    /**
     * Send message
     *
     * @param op
     */
    void message(GraphOp op);

    /**
     * Storage Attached to this GNNLayer
     *
     * @return
     */
    BaseStorage getStorage();


    /**
     * Is this the first GNN-layer
     *
     * @return
     */
    default boolean isFirst() {
        return getPosition() == 1;
    }

    ;

    /**
     * Is this the last or output GNN-layer
     *
     * @return
     */
    default boolean isLast() {
        return getPosition() >= getLayers();
    }

    /**
     * Process incoming value
     * @param value
     */
    default void process(GraphOp value) {
        try {
            switch (value.op) {
                case COMMIT:
                    GraphElement thisElement = getStorage().getElement(value.element);
                    if (Objects.isNull(thisElement)) {
                        if (!this.isLast() && (value.element.elementType() == ElementType.EDGE || value.element.elementType() == ElementType.VERTEX)) {
                            message(new GraphOp(Op.COMMIT, this.getCurrentPart(), value.element.copy(), IterationType.FORWARD));
                        }
                        value.element.setStorage(getStorage());
                        value.element.createElement();
                    } else {
                        thisElement.externalUpdate(value.element);
                    }
                    break;
                case SYNC:
                    GraphElement el = this.getStorage().getElement(value.element);
                    if (Objects.isNull(el)) {
                        if (!this.isLast() && (value.element.elementType() == ElementType.EDGE || value.element.elementType() == ElementType.VERTEX)) {
                            message(new GraphOp(Op.COMMIT, this.getCurrentPart(), value.element.copy(), IterationType.FORWARD));
                        }
                        el = value.element.copy();
                        el.setStorage(getStorage());
                        el.createElement();
                        if (el.state() == ReplicaState.MASTER) el.syncElement(value.element);
                    } else {
                        el.syncElement(value.element);
                    }
                    break;
                case RMI:
                    GraphElement rpcElement = this.getStorage().getElement(value.element.getId(), value.element.elementType());
                    Rmi.execute(rpcElement, (Rmi) value.element);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            getStorage().manager.clean();
        }
    }

}
