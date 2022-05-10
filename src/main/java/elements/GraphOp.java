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
     * @param ts
     */
    public GraphOp(Op op, short part_id, GraphElement element,  long ts) {
        this.op = op;
        this.part_id = part_id;
        this.element = element;
        this.ts = ts;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
        this.op = op;
    }

    public short getPart_id() {
        return part_id;
    }

    public void setPart_id(short part_id) {
        this.part_id = part_id;
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

    public boolean isTopologicalUpdate() {
        return op == Op.COMMIT && (element.elementType() == ElementType.EDGE || element.elementType() == ElementType.VERTEX);
    }

    public GraphOp copy() {
        return new GraphOp(this.op, this.part_id, this.element, this.ts);
    }

    @Override
    public String toString() {
        return "GraphOp{" +
                "op=" + op +
                ", part_id=" + part_id +
                ", element=" + element +
                ", ts=" + ts +
                '}';
    }
}
