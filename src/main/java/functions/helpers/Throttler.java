package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Throttler extends ProcessFunction<GraphOp, GraphOp> {
    private final long nanoSeconds;
    private final long maxRecords;
    private long count = 0;
    public Throttler(int numRecordsPerSecond, long maxRecords) {
        nanoSeconds = (long) (1000000000f / numRecordsPerSecond);
        this.maxRecords = maxRecords;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        if(count++ > maxRecords) return;
        long startTime = System.nanoTime();
        while (System.nanoTime() <= startTime + nanoSeconds) {
            // continue
            Thread.onSpinWait();
        }

        out.collect(value);
    }
}
