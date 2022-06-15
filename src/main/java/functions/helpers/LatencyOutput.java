package functions.helpers;

import elements.GraphOp;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class LatencyOutput extends KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void> {
    private final int movingAverageSize;
    private transient MyGauge gauge;
    private transient DescriptiveStatisticsHistogram histogram;
    private transient HashMap<Long, Long> requestLatencies;

    public LatencyOutput(int movingAverageSize) {
       this.movingAverageSize = movingAverageSize;
    }

    @Override
    public void processElement1(GraphOp value, KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        requestLatencies.put(value.getTimestamp(), ctx.timerService().currentProcessingTime());
    }

    @Override
    public void processElement2(GraphOp value, KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        if(requestLatencies.containsKey(value.getTimestamp())){
            int latency = (int) (ctx.timerService().currentProcessingTime() - requestLatencies.get(value.getTimestamp()));
            gauge.add(latency);
            histogram.update(latency);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        gauge = new MyGauge(movingAverageSize);
        histogram = new DescriptiveStatisticsHistogram(3000);
        requestLatencies = new HashMap<>();
        getRuntimeContext().getMetricGroup().gauge(String.format("%s-Maverage-latency",movingAverageSize),gauge);
        getRuntimeContext().getMetricGroup().histogram(String.format("histogram-latency"),histogram);
    }

    public static class MyGauge implements Gauge<Integer>{
        private transient CircularFifoBuffer buffer;
        public MyGauge(int capacity){
            buffer = new CircularFifoBuffer(capacity);
        }
        public void add(int latency){
            buffer.add(latency);
        }

        @Override
        public Integer getValue() {
            if(buffer.isEmpty())return 0;
            Integer sum = (Integer) buffer.parallelStream().reduce(1, (a,b)->((int)a + (int) b));
            long latencyOutput = sum/buffer.size();
            return (int) latencyOutput;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
