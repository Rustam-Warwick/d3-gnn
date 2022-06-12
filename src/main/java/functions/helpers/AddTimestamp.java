package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class AddTimestamp extends CoProcessFunction<GraphOp, GraphOp, GraphOp> {

    @Override
    public void processElement1(GraphOp value, CoProcessFunction<GraphOp, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        value.setTimestamp(ctx.timestamp());
        out.collect(value);
    }

    @Override
    public void processElement2(GraphOp value, CoProcessFunction<GraphOp, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        value.setTimestamp(ctx.timestamp());
        out.collect(value);
    }
}
