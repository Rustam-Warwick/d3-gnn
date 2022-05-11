package partitioner;

import elements.Edge;
import elements.ElementType;
import elements.GraphOp;
import elements.Vertex;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPartitioner extends BasePartitioner {
    public HashMap<String, Short> masters = new HashMap<String, Short>();

    @Override
    public boolean isParallel() {
        return false;
    }

    @Override
    public GraphOp map(GraphOp value) throws Exception {
        if (value.element.elementType() == ElementType.EDGE) {
            value.partId = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
            Edge edge = (Edge) value.element;
            this.masters.putIfAbsent(edge.src.getId(), value.partId);
            this.masters.putIfAbsent(edge.dest.getId(), value.partId);
            edge.src.master = this.masters.get(edge.src.getId());
            edge.dest.master = this.masters.get(edge.dest.getId());
        } else if (value.element.elementType() == ElementType.VERTEX) {
            short part_tmp = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
            this.masters.putIfAbsent(value.element.getId(), part_tmp);
            ((Vertex) value.element).master = this.masters.get(value.element.getId());
            value.partId = part_tmp;
        }
        return value;
    }
}
