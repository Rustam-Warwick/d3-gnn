package functions.helpers;

import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class LatencyFileOutput extends ProcessFunction<GraphOp, GraphOp> {
    public final String jobName;
    public transient HashMap<Long, Long> latenciesTable;

    public LatencyFileOutput(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        latenciesTable = new HashMap<>(100000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        String homePath = System.getenv("HOME");
        File outputFile = new File(String.format("%s/metrics/%s/%s-latency.csv", homePath, jobName, getRuntimeContext().getTaskNameWithSubtasks()));
        File parent = outputFile.getParentFile();
        try {
            parent.mkdirs();
            outputFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }
        for (Map.Entry<Long, Long> longLongEntry : latenciesTable.entrySet()) {
            Files.write(outputFile.toPath(), String.format("%s,%s\n", longLongEntry.getKey(), longLongEntry.getValue()).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        }
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        latenciesTable.put(value.getTimestamp(), ctx.timerService().currentProcessingTime());
    }
}
