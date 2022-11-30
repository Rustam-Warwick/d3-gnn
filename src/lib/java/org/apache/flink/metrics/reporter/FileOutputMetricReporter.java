package org.apache.flink.metrics.reporter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric Reporter that ouputs to a file all the task metrics
 */
public class FileOutputMetricReporter implements Scheduled, MetricReporter {
    public static List<String> names = List.of("windowThroughput", "accuracy", "latency", "epochThroughput", "lossValue", "throughput", "numRecordsOut", "numRecordsIn", "numRecordsInPerSecond", "numRecordsOutPerSecond", "idleTimeMsPerSecond", "busyTimeMsPerSecond", "backPressuredTimeMsPerSecond", "Replication Factor");

    public ConcurrentHashMap<Metric, Tuple2<File, StringBuilder>> fileHashMap = new ConcurrentHashMap<>(100);

    @Override
    public void open(MetricConfig config) {
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {

        if (fileHashMap.containsKey(metric)) {
            Tuple2<File, StringBuilder> file = fileHashMap.get(metric);
            if (metric instanceof Gauge) {
                Gauge tmp = (Gauge) metric;
                file.f1.append(tmp.getValue().toString()).append("\n");
            }
            if (metric instanceof Counter) {
                Counter tmp = (Counter) metric;
                file.f1.append(tmp.getCount()).append("\n");
            }
            if (metric instanceof Meter) {
                Meter tmp = (Meter) metric;
                file.f1.append(tmp.getRate()).append("\n");
            }
            try {
                Files.write(file.f0.toPath(), file.f1.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                fileHashMap.remove(metric);
            }
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        createFileForMetric(metric, metricName, group);
    }

    @Override
    public void close() {
        fileHashMap.clear();
    }

    @Override
    public void report() {
        fileHashMap.forEach((metric, values) -> {
            if (metric instanceof Gauge) {
                Gauge tmp = (Gauge) metric;
                values.f1.append(tmp.getValue().toString()).append("\n");
            }
            if (metric instanceof Counter) {
                Counter tmp = (Counter) metric;
                values.f1.append(tmp.getCount()).append("\n");
            }
            if (metric instanceof Meter) {
                Meter tmp = (Meter) metric;
                values.f1.append(tmp.getRate()).append("\n");
            }
        });
    }

    public synchronized void createFileForMetric(Metric metric, String metricName, MetricGroup group) {
        StringBuilder str = new StringBuilder();
        Map<String, String> variables = group.getAllVariables();
        if (names.contains(metricName) && variables.containsKey("<job_name>") && variables.containsKey("<operator_name>") && variables.containsKey("<subtask_index>")) {
            str.append(variables.get("<job_name>"));
            str.append('/');
            str.append(variables.get("<operator_name>"));
            str.append('/');
            str.append(metricName);
            str.append('/');
            str.append(variables.get("<subtask_index>"));
            File file = new File(String.format("%s/metrics/%s", System.getenv("HOME"), str));
            File parent = file.getParentFile();
            try {
                parent.mkdirs();
                file.createNewFile();
                fileHashMap.put(metric, new Tuple2<>(file, new StringBuilder()));
            } catch (IllegalStateException | IOException e) {
                e.printStackTrace();
            }
        }
    }

}
