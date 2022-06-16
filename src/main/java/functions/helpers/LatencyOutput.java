package functions.helpers;

import elements.GraphOp;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

/**
 * Output the latency and histogram of the stream
 */
public class LatencyOutput extends KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void> {

    private final int movingAverageSize; // Size of the moving average latency

    private final String jobName; // Name of the job to organize files

    private transient File outputLatenciesFile; // File for Raw latencies output

    private transient File outputMovingAverageFile; // File for Moving Average metrics

    private transient MyGauge gauge; // Gauge Metric

    private final HashMap<Long, Long> requestLatencies = new HashMap<>(10000); // HashMap for initial request timestamps

    public LatencyOutput() {
        this.movingAverageSize = 100;
        this.jobName = null;
    }

    public LatencyOutput(String jobName, int movingAverageSize) {
        this.movingAverageSize = movingAverageSize;
        this.jobName = jobName;

    }

    @Override
    public void processElement1(GraphOp value, KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
       requestLatencies.put(value.getTimestamp(), ctx.timerService().currentProcessingTime());
    }

    @Override
    public void processElement2(GraphOp value, KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        if (requestLatencies.containsKey(value.getTimestamp())) {
            int latency = (int) (ctx.timerService().currentProcessingTime() - requestLatencies.get(value.getTimestamp()));
            gauge.add(latency);
            Files.write(outputLatenciesFile.toPath(), String.format("%s,%s\n", value.getTimestamp(), latency).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        gauge = new MyGauge(movingAverageSize);
        getRuntimeContext().getMetricGroup().gauge(String.format("%s-Maverage-latency", movingAverageSize), gauge);
        String homePath = System.getenv("HOME");
        outputLatenciesFile = new File(String.format("%s/metrics/%s/latencies-%s.csv", homePath, jobName,getRuntimeContext().getIndexOfThisSubtask()));
        outputMovingAverageFile = new File(String.format("%s/metrics/%s/%s-Maverage-latency-%s.csv", homePath, jobName,movingAverageSize, getRuntimeContext().getIndexOfThisSubtask()));
        File parent = outputLatenciesFile.getParentFile();
        try {
            parent.mkdirs();
            outputLatenciesFile.createNewFile();
            outputMovingAverageFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public class MyGauge implements Gauge<Integer> {
        private final transient CircularFifoBuffer buffer;

        public MyGauge(int capacity) {
            buffer = new CircularFifoBuffer(capacity);
        }

        public void add(int latency) {
            buffer.add(latency);
        }

        @Override
        public Integer getValue() {
            try{
                if (buffer.isEmpty()) return 0;
                Integer sum = (Integer) buffer.parallelStream().reduce(1, (a, b) -> ((int) a + (int) b));
                long latencyOutput = sum / buffer.size();
                Files.write(outputMovingAverageFile.toPath(), (latencyOutput + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                return (int) latencyOutput;
            }catch (Exception e){
                e.printStackTrace();
                return 0;
            }
        }
    }

}
