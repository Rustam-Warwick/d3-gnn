package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class AddTimestamp extends ProcessFunction<GraphOp, GraphOp> {
    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        value.setTimestamp(ctx.timestamp());
        out.collect(value);
    }
}
