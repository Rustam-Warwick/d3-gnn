package functions.filewriters;

import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class ThroughputMetricsOutput extends ProcessFunction<GraphOp, Void> {
    private final String jobName;
    List<Double> values = new ArrayList<>(1000);
    private File outputFile;
    private InternalOperatorMetricGroup metricGroup;
    private int count;

    public ThroughputMetricsOutput(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        count++;
        if (count == 1000) {
            values.add(metricGroup.getIOMetricGroup().getNumRecordsInRateMeter().getRate());
            if (values.size() == 1000) {
                writeToFile();
            }
            count = 0;
        }
        LifeCycleNDManager.getInstance().clean();
    }

    private void writeToFile() throws Exception {
        if (values.isEmpty()) return;
        StringBuilder a = new StringBuilder();
        for (Double latency : values) {
            a.append(latency);
            a.append('\n');
        }
        Files.write(outputFile.toPath(), a.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        values.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        writeToFile();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        metricGroup = (InternalOperatorMetricGroup) getRuntimeContext().getMetricGroup();
        String homePath = System.getenv("HOME");
        outputFile = new File(String.format("%s/metrics/%s/embeddings-throughput-%s.csv", homePath, jobName, getRuntimeContext().getIndexOfThisSubtask()));
        File parent = outputFile.getParentFile();
        try {
            parent.mkdirs();
            outputFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }
    }
}
