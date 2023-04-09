package org.apache.flink.metrics.reporter;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.*;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metric Reporter that ouputs to a file all the task metrics
 */
public class FileOutputMetricReporter implements Scheduled, MetricReporter {

    protected static final List<String> operatorMetricNames = List.of("windowThroughput", "accuracy", "latency", "epochThroughput", "lossValue", "throughput", "Replication Factor", "numRecordsOut", "numRecordsIn", "numRecordsInPerSecond", "numRecordsOutPerSecond");
    protected static final List<String> taskMetricNames = List.of("busyTimeMsPerSecond", "numBytesInLocal", "numBytesInRemote");
    private static final long FLUSH_DELTA = 120000;
    protected final Map<String, Tuple3<File, Metric, StringBuilder>> metricsMap = new HashMap<>(100);
    protected long lastFlush = Long.MIN_VALUE;

    @Override
    public void open(MetricConfig config) {
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        String fileName = getMetricFileName(metric, metricName, group);
        if(fileName != null) {
            Tuple3<File, Metric, StringBuilder> tmp = null;
            synchronized (this) {
                tmp = metricsMap.remove(fileName);
            }
            if(tmp != null){
                appendMetric(tmp);
                flushMetric(tmp);
            }
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String fileName = getMetricFileName(metric, metricName, group);
        if (fileName != null) createFileForMetric(metric, metricName, group, fileName);
    }

    @Override
    public void close() {
        metricsMap.clear();
    }

    @Override
    public void report() {
        if (lastFlush == Long.MIN_VALUE) lastFlush = System.currentTimeMillis();
        final boolean withFlush = (System.currentTimeMillis() - lastFlush) > FLUSH_DELTA;
        metricsMap.forEach((fileName, values) -> {
            appendMetric(values);
            if (withFlush) {
                flushMetric(values);
                values.f2 = new StringBuilder();
            }
        });
        if (withFlush) lastFlush = System.currentTimeMillis();
    }

    protected void appendMetric(Tuple3<File, Metric, StringBuilder> values) {
        Metric metric = values.f1;
        if (metric instanceof Gauge) {
            Gauge tmp = (Gauge) metric;
            values.f2.append(tmp.getValue().toString()).append("\n");
        } else if (metric instanceof Counter) {
            Counter tmp = (Counter) metric;
            values.f2.append(tmp.getCount()).append("\n");
        } else if (metric instanceof Meter) {
            Meter tmp = (Meter) metric;
            values.f2.append(tmp.getRate()).append("\n");
        }
    }

    protected void flushMetric(Tuple3<File, Metric, StringBuilder> values) {
        try {
            Files.write(values.f0.toPath(), values.f2.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Nullable
    protected String getMetricFileName(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> variables = group.getAllVariables();
        int subtaskIndex = Integer.parseInt(variables.getOrDefault("<subtask_index>", "-1"));
        if (subtaskIndex == -1) return null;
        if (operatorMetricNames.contains(metricName) && variables.containsKey("<operator_name>")) {
            return String.format("%s/%s/%s/%s", variables.get("<job_name>"), variables.get("<operator_name>"), metricName, subtaskIndex);
        }
        if (taskMetricNames.contains(metricName) && !variables.containsKey("<operator_name>")) {
            return String.format("%s/Task-%s/%s/%s", variables.get("<job_name>"), variables.get("<task_name>"), metricName, subtaskIndex);
        }
        return null;
    }

    synchronized protected void createFileForMetric(Metric metric, String metricName, MetricGroup group, String fileName) {
        try {
            File file = new File(String.format("%s/metrics/%s", System.getenv("HOME"), fileName));
            File parent = file.getParentFile();
            parent.mkdirs();
            file.createNewFile();
            metricsMap.putIfAbsent(fileName, new Tuple3<>(file, metric, new StringBuilder()));
        } catch (IllegalStateException | IOException e) {
            e.printStackTrace();
        }
    }

}
