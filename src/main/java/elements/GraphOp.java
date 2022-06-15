package elements;

import elements.iterations.MessageCommunication;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * Main message object that gets passed around the system
 */
public class GraphOp {
    /**
     * @see Op
     * Op represents the operation that is happening in the GraphElement
     */
    public Op op;
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
    public OperatorEvent operatorEvent;
    /**
     * Type of communication message Part-to-Part or Broadcast
     */
    public MessageCommunication messageCommunication = MessageCommunication.P2P; // Point-to-Point messages
    /**
     * Timestamp associated with this GraphOp
     * Mainly used for Watermarks
     */
    public Long ts = null;

    public GraphOp() {
        this.op = Op.COMMIT;
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

    public GraphOp(Op op, Short partId, GraphElement element, Long ts, MessageCommunication communication) {
        this(op, partId, element, null, communication, ts);
    }

    public GraphOp(Op op, Short partId, GraphElement element, OperatorEvent operatorEvent, MessageCommunication messageCommunication, Long ts) {
        this.op = op;
        this.partId = partId;
        this.element = element;
        this.operatorEvent = operatorEvent;
        this.messageCommunication = messageCommunication;
        this.ts = ts;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
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

    public MessageCommunication getMessageCommunication() {
        return messageCommunication;
    }

    public void setMessageCommunication(MessageCommunication messageCommunication) {
        this.messageCommunication = messageCommunication;
    }

    public OperatorEvent getOperatorEvent() {
        return operatorEvent;
    }

    public void setOperatorEvent(OperatorEvent operatorEvent) {
        this.operatorEvent = operatorEvent;
    }

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
    }

    public GraphOp copy() {
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
