package functions.helpers;

import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LatencyOutput extends KeyedProcessFunction<Long, GraphOp, Void> {
    private final HashMap<Long, List<Long>> requestLatencies = new HashMap<>();

    private final String outputFileName;
    private transient long sumLatency;
    private transient int N;

    public LatencyOutput(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<Long, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        requestLatencies.computeIfAbsent(value.getTimestamp(), (key) -> new ArrayList<>());
        requestLatencies.compute(value.getTimestamp(), (key, val) -> {
            if (!val.isEmpty()) {
                sumLatency += Math.abs(val.get(0) - ctx.timerService().currentProcessingTime());
            }
            val.add(ctx.timerService().currentProcessingTime());
            return val;
        });
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().getMetricGroup().gauge("MyLatency", new MyGauge());
    }

    @Override
    public void close() throws Exception {
        super.close();
        String homePath = System.getenv("HOME");
        File outputFile = new File(String.format("%s/metrics/%s/latencies-%s.csv", homePath, outputFileName, getRuntimeContext().getIndexOfThisSubtask()));
        File parent = outputFile.getParentFile();

        try {
            parent.mkdirs();
            outputFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }

        StringBuilder builder = new StringBuilder();
        requestLatencies.forEach((timestamp, processingTimeLists) -> {
            for (Long processingTime : processingTimeLists) {
                builder.append(String.format("%s,%s\n", timestamp, processingTime));
            }
        });

        Files.write(outputFile.toPath(), builder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }

    public class MyGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            long latencyOutput = (long) sumLatency / N;
            sumLatency = 0;
            N = 0;
            return latencyOutput;
        }
    }
}
