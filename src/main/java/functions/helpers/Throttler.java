package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Throttler extends ProcessFunction<GraphOp, GraphOp> {
    private final long nanoSeconds;

    public Throttler(int numRecordsPerSecond) {
        nanoSeconds = (long) (1f / numRecordsPerSecond * 1000000000);
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        long startTime = System.nanoTime();
        while (System.nanoTime() < startTime + nanoSeconds) {
            // continue
        }
        out.collect(value);
    }
}
