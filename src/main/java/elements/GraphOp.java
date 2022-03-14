package elements;

import iterations.IterationState;

public class GraphOp {
    public Op op;
    public short part_id = -1;
    public GraphElement element = null;
    public IterationState state = IterationState.FORWARD;
    public GraphOp(){
        this.op = Op.COMMIT;
    }
    public GraphOp(Op op, GraphElement element) {
        this.op = op;
        this.element = element;
    }

    public GraphOp(Op op, short part_id, GraphElement element, IterationState state) {
        this.op = op;
        this.part_id = part_id;
        this.element = element;
        this.state = state;
    }

    public GraphOp copy(){
        return new GraphOp(this.op, this.part_id, this.element.copy(), this.state);
    }

    public boolean isTopologyChange(){
        return (this.op == Op.COMMIT || this.op == Op.REMOVE) && (this.element.elementType() == ElementType.EDGE || (this.element.elementType() == ElementType.VERTEX));
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
