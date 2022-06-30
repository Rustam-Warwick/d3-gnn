package functions.filewriters;

import elements.Edge;
import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * Write the partitioned graph data to a CSV File
 */
public class PartitionerCsvFileWriter extends ProcessFunction<GraphOp, Void> {
    private final String jobName;
    private transient File outputFile;

    public PartitionerCsvFileWriter(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
        Edge e = (Edge) value.getElement();
        Files.write(outputFile.toPath(), String.format("%s,%s,%s,%s,%s,%s\n", ctx.timestamp(), e.src.getId(), e.src.masterPart(), e.dest.getId(), e.dest.masterPart(), value.getPartId()).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String homePath = System.getenv("HOME");
        outputFile = new File(String.format("%s/metrics/%s/partitioned_data.csv", homePath, jobName));
        File parent = outputFile.getParentFile();
        try {
            parent.mkdirs();
            outputFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }
    }
}
