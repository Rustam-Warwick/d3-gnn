package elements;

import iterations.MessageDirection;

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
    public short part_id = -1;
    /**
     * The GraphElement on which the Op is being acted upon
     */
    public GraphElement element = null;
    /**
     * The direction to where this message should be sent to
     */
    public MessageDirection direction = MessageDirection.FORWARD;
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
     *
     * @param op
     * @param part_id
     * @param element
     * @param direction
     * @param ts
     */
    public GraphOp(Op op, short part_id, GraphElement element, MessageDirection direction, long ts) {
        this.op = op;
        this.part_id = part_id;
        this.element = element;
        this.direction = direction;
        this.ts = ts;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long ts) {
        this.ts = ts;
    }

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
    }


    public GraphOp copy() {
        return new GraphOp(this.op, this.part_id, this.element, this.direction, this.ts);
    }

    @Override
    public String toString() {
        return "GraphOp{" +
                "op=" + op +
                ", part_id=" + part_id +
                ", element=" + element +
                ", direction=" + direction +
                ", ts=" + ts +
                '}';
    }
}
