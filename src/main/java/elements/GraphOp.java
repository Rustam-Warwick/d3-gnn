package elements;

import iterations.IterationType;

public class GraphOp {
    public Op op;
    public short part_id = -1;
    public GraphElement element = null;
    public IterationType state = IterationType.FORWARD;
    public int ts = Integer.MIN_VALUE;

    public GraphOp() {
        this.op = Op.COMMIT;
    }

    public GraphOp(Op op, GraphElement element) {
        this.op = op;
        this.element = element;
    }

    public GraphOp(Op op, short part_id, GraphElement element, IterationType state) {
        this.op = op;
        this.part_id = part_id;
        this.element = element;
        this.state = state;
    }

    public GraphOp(Op op, short part_id, GraphElement element, IterationType state, int ts) {
        this.op = op;
        this.part_id = part_id;
        this.element = element;
        this.state = state;
        this.ts = ts;
    }

    public int getTimestamp() {
        return ts;
    }

    public void setTimestamp(int ts) {
        this.ts = ts;
    }

    public GraphOp copy() {
        return new GraphOp(this.op, this.part_id, this.element, this.state);
    }

    @Override
    public String toString() {
        return "GraphOp{" +
                "op=" + op +
                ", part_id=" + part_id +
                ", element=" + element +
                ", state=" + state +
                '}';
    }
}
