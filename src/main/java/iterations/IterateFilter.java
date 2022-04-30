package iterations;

import elements.GraphOp;
import org.apache.flink.api.common.functions.FilterFunction;

public class IterateFilter implements FilterFunction<GraphOp> {
    @Override
    public boolean filter(GraphOp value) throws Exception {
        return value.direction == MessageDirection.ITERATE;
    }
}
