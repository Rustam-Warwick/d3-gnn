package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Throttler extends ProcessFunction<GraphOp, GraphOp> {
    private final int sleepCycle;
    public Throttler(int numRecordsPerSecond) {
        sleepCycle = 1000/numRecordsPerSecond;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        Thread.sleep(1);
        out.collect(value);
    }
}
