package iterations;

import elements.GraphOp;
import org.apache.flink.api.common.functions.FilterFunction;

public class ForwardFilter implements FilterFunction<GraphOp> {
    @Override
    public boolean filter(GraphOp value) throws Exception {
        return value.state == IterationState.FORWARD;
    }
}
