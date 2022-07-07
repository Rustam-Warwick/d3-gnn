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

    public GraphOp(BaseOperatorEvent evt){
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

    public @NotNull Op getOp() {
        return op;
    }

    public void setOp(@NotNull Op op) {
        this.op = op;
    }

    public Short getPartId() {
        return partId;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

    public GraphElement getElement() {
        return element;
    }

    public void setElement(GraphElement element) {
        this.element = element;
    }

    public Long getTimestamp() {
        return ts;
    }

    public void setTimestamp(Long ts) {
        this.ts = ts;
    }

    public @NotNull MessageCommunication getMessageCommunication() {
        return messageCommunication;
    }

    public void setMessageCommunication(@NotNull MessageCommunication messageCommunication) {
        this.messageCommunication = messageCommunication;
    }

    public BaseOperatorEvent getOperatorEvent() {
        return operatorEvent;
    }

    public void setOperatorEvent(BaseOperatorEvent operatorEvent) {
        this.operatorEvent = operatorEvent;
    }

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
    }

    public GraphOp shallowCopy() {
        return
                new GraphOp(this.op, this.partId, this.element, this.operatorEvent, this.messageCommunication, this.ts);
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
