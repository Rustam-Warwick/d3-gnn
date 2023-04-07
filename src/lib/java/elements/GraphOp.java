package elements;

import ai.djl.ndarray.LifeCycleControl;
import elements.enums.MessageCommunication;
import elements.enums.Op;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.jetbrains.annotations.NotNull;
import typeinfo.graphopinfo.GraphOpTypeInfoFactory;

import java.util.Objects;

/**
 * Main output object that gets sent around the system
 * <p>
 * There are not getters for this element since all the fields are populated and there is no internal logic to its fields
 * </p>
 */
@SuppressWarnings("UnusedReturnValue")
@TypeInfo(GraphOpTypeInfoFactory.class)
public final class GraphOp implements LifeCycleControl {

    /**
     * {@link Op} represents the operation that is happening in the GraphElement
     */
    public Op op = Op.COMMIT;

    /**
     * The part number where this record should be sent to
     */
    public short partId = -1;

    /**
     * The GraphElement on which the Op is being acted upon
     */
    public GraphElement element;

    /**
     * Operator Event for the plugins communicated through this channel
     */
    public GraphEvent graphEvent;

    /**
     * Type of communication output Part-to-Part or Broadcast
     */
    public @NotNull
    MessageCommunication messageCommunication = MessageCommunication.P2P; // Point-to-Point messages

    /**
     * Timestamp associated with this GraphOp
     * Mainly used for Watermarks
     */
    public Long ts;

    public GraphOp() {
    }

    public GraphOp(Op op, GraphElement element) {
        this.op = op;
        this.element = element;
    }

    public GraphOp(Op op, short partId, GraphElement element) {
        this.op = op;
        this.partId = partId;
        this.element = element;
    }

    public GraphOp(GraphEvent evt) {
        this.op = Op.OPERATOR_EVENT;
        this.graphEvent = evt;
        this.messageCommunication = MessageCommunication.BROADCAST;
    }

    public GraphOp(Op op, short partId, GraphElement element, GraphEvent graphEvent, @NotNull MessageCommunication messageCommunication, Long ts) {
        this.op = op;
        this.partId = partId;
        this.element = element;
        this.graphEvent = graphEvent;
        this.messageCommunication = messageCommunication;
        this.ts = ts;
    }

    public GraphOp copy() {
        return new GraphOp(this.op, this.partId, this.element, this.graphEvent, this.messageCommunication, this.ts);
    }

    public GraphOp setOp(@NotNull Op op) {
        this.op = op;
        return this;
    }

    public GraphOp setPartId(short partId) {
        this.partId = partId;
        return this;
    }

    public GraphOp setElement(GraphElement element) {
        this.element = element;
        return this;
    }

    public GraphOp setTimestamp(Long ts) {
        this.ts = ts;
        return this;
    }

    public GraphOp setMessageCommunication(@NotNull MessageCommunication messageCommunication) {
        this.messageCommunication = messageCommunication;
        return this;
    }

    public GraphOp setGraphEvent(GraphEvent graphEvent) {
        this.graphEvent = graphEvent;
        return this;
    }

    // OVERRIDE METHODS

    @Override
    public void resume() {
        if (element != null) element.resume();
    }

    @Override
    public void delay() {
        if (element != null) element.delay();
    }

    @Override
    public void destroy() {
        if (element != null) element.destroy();
    }

    @Override
    public String toString() {
        return "GraphOp{" +
                "op=" + op +
                ", part_id=" + partId +
                ", element=" + element +
                ", ts=" + ts +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphOp graphOp = (GraphOp) o;
        return op == graphOp.op && Objects.equals(element, graphOp.element) && Objects.equals(graphEvent, graphOp.graphEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, element, graphEvent);
    }

}
