package partitioner;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

public class PartKeySelector implements KeySelector<GraphOp, Short> {
    @Override
    public Short getKey(GraphOp value) throws Exception {
        return (short) (value.part_id * 1000);
    }
}
