package functions.helpers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import elements.GraphOp;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

/**
 * Output the latency and histogram of the stream
 */
public class LatencyOutput extends KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void> {
    private final int movingAverageSize;
    private final String jobName;
    private transient File outputLatenciesFile;
    private transient File outputGaugeFile;
    private transient MyGauge gauge;
    private transient org.apache.flink.metrics.Histogram histogram;
    private transient long[] requestLatencies;

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
        requestLatencies[Math.toIntExact(value.getTimestamp())] = ctx.timerService().currentProcessingTime();
    }

    @Override
    public void processElement2(GraphOp value, KeyedCoProcessFunction<Long, GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        int index = Math.toIntExact(value.getTimestamp());
        if (requestLatencies[index]!=0) {
            int latency = (int) (ctx.timerService().currentProcessingTime() - requestLatencies[index]);
            gauge.add(latency);
            histogram.update(latency);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        gauge = new MyGauge(movingAverageSize);
        histogram = new DropwizardHistogramWrapper(new Histogram(new SlidingWindowReservoir(3000)));
        requestLatencies = new long[1000000];
        getRuntimeContext().getMetricGroup().gauge(String.format("%s-Maverage-latency", movingAverageSize), gauge);
        getRuntimeContext().getMetricGroup().histogram("histogram-latency", histogram);

        String homePath = System.getenv("HOME");
        outputLatenciesFile = new File(String.format("%s/metrics/%s/latencies-%s.csv", homePath, jobName,getRuntimeContext().getIndexOfThisSubtask()));
        outputGaugeFile = new File(String.format("%s/metrics/%s/%s-Maverage-latency-%s.csv", homePath, jobName,movingAverageSize, getRuntimeContext().getIndexOfThisSubtask()));
        File parent = outputLatenciesFile.getParentFile();
        try {
            parent.mkdirs();
            outputLatenciesFile.createNewFile();
            outputGaugeFile .createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void close() throws Exception {

        super.close();
    }

    public static class MyGauge implements Gauge<Integer> {
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

                Files.write(outputFile.toPath(), builder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                return (int) latencyOutput;
            }catch (Exception e){
                e.printStackTrace();
                return 0;
            }
        }
    }

}
