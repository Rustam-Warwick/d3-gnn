package elements;

import elements.iterations.MessageCommunication;

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
    public short partId = -1;
    /**
     * The GraphElement on which the Op is being acted upon
     */
    public GraphElement element = null;
    /**
     * Type of communication message Part-to-Part or Broadcast
     */
    public MessageCommunication messageCommunication = MessageCommunication.P2P; // Point-to-Point messages
    /**
     * Timestamp associated with this GraphOp
     * Mainly used for Watermarks
     */
    public long ts;

    public GraphOp() {
        this.op = Op.COMMIT;
    }

    /**
     * Constructed used initially
     *
     * @param op      Op
     * @param element GraphElement
     */
    public GraphOp(Op op, GraphElement element, long ts) {
        this.op = op;
        this.element = element;
        this.ts = ts;
    }

    /**
     * Constructor to be used in the system
     */
    public GraphOp(Op op, short partId, GraphElement element, long ts) {
        this.op = op;
        this.partId = partId;
        this.element = element;
        this.ts = ts;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
        this.op = op;
    }

    public short getPartId() {
        return partId;
    }

    public void setPartId(short partId) {
        this.partId = partId;
    }

    public GraphElement getElement() {
        return element;
    }

    public void setElement(GraphElement element) {
        this.element = element;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long ts) {
        this.ts = ts;
    }

    public MessageCommunication getMessageCommunication() {
        return messageCommunication;
    }

    public void setMessageCommunication(MessageCommunication messageCommunication) {
        this.messageCommunication = messageCommunication;
    }

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
    }

    public GraphOp copy() {
        return new GraphOp(this.op, this.partId, this.element, this.ts);
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
}
