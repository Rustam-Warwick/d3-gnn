package elements;

import elements.iterations.MessageCommunication;
import operators.events.BaseOperatorEvent;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.jetbrains.annotations.NotNull;
import typeinfo.GraphOpTypeInfoFactory;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Main message object that gets passed around the system
 */
@TypeInfo(GraphOpTypeInfoFactory.class)
public final class GraphOp {
    /**
     * @see Op
     * Op represents the operation that is happening in the GraphElement
     */
    public @Nonnull
    Op op = Op.NONE;
    /**
     * The part number where this record should be sent to
     */
    public Short partId;
    /**
     * The GraphElement on which the Op is being acted upon
     */
    public GraphElement element;
    /**
     * Operator Event for the plugins communicated through this channel
     */
    public BaseOperatorEvent operatorEvent;
    /**
     * Type of communication message Part-to-Part or Broadcast
     */
    public @Nonnull
    MessageCommunication messageCommunication = MessageCommunication.P2P; // Point-to-Point messages
    /**
     * Timestamp associated with this GraphOp
     * Mainly used for Watermarks
     */
    public Long ts;

    public GraphOp() {

    }

    public GraphOp(Op op, GraphElement element) {
        this(op, element, null);
    }

    public GraphOp(Op op, GraphElement element, Long ts) {
        this(op, null, element, ts);
    }

    public GraphOp(Op op, Short partId, GraphElement element) {
        this(op, partId, element, null);
    }

    public GraphOp(Op op, Short partId, GraphElement element, Long ts) {
        this(op, partId, element, ts, MessageCommunication.P2P);
    }

    public GraphOp(BaseOperatorEvent evt) {
        this(Op.OPERATOR_EVENT, null, null, evt, MessageCommunication.BROADCAST, null);
    }

    public GraphOp(Op op, Short partId, GraphElement element, Long ts, MessageCommunication communication) {
        this(op, partId, element, null, communication, ts);
    }

    public GraphOp(@NotNull Op op, Short partId, GraphElement element, BaseOperatorEvent operatorEvent, @NotNull MessageCommunication messageCommunication, Long ts) {
        this.op = op;
        this.partId = partId;
        this.element = element;
        this.operatorEvent = operatorEvent;
        this.messageCommunication = messageCommunication;
        this.ts = ts;
    }

    public GraphOp shallowCopy() {
        return
                new GraphOp(this.op, this.partId, this.element, this.operatorEvent, this.messageCommunication, this.ts);
    }

    // --- GETTERS AND SETTERS
    public @NotNull Op getOp() {
        return op;
    }

    public GraphOp setOp(@NotNull Op op) {
        this.op = op; return this;
    }

    public Short getPartId() {
        return partId;
    }

    public GraphOp setPartId(Short partId) {
        this.partId = partId;
        return this;
    }

    public GraphElement getElement() {
        return element;
    }

    public GraphOp setElement(GraphElement element) {
        this.element = element;
        return this;
    }

    public Long getTimestamp() {
        return ts;
    }

    public GraphOp setTimestamp(Long ts) {
        this.ts = ts;
        return this;
    }

    public @NotNull MessageCommunication getMessageCommunication() {
        return messageCommunication;
    }

    public GraphOp setMessageCommunication(@NotNull MessageCommunication messageCommunication) {
        this.messageCommunication = messageCommunication;
        return this;
    }

    public BaseOperatorEvent getOperatorEvent() {
        return operatorEvent;
    }

    public GraphOp setOperatorEvent(BaseOperatorEvent operatorEvent) {
        this.operatorEvent = operatorEvent;
        return this;
    }

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
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
        return op == graphOp.op && Objects.equals(element, graphOp.element) && Objects.equals(operatorEvent, graphOp.operatorEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, element, operatorEvent);
    }
}
