package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.*;

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
public class FileOutputMetricReporter  implements Scheduled, MetricReporter {
    public HashMap<Metric, File> fileHashMap = new HashMap<>(1000);
    public static List<String> names = List.of("numRecordsInPerSecond","numRecordsOutPerSecond","idleTimeMsPerSecond", "busyTimeMsPerSecond", "backPressuredTimeMsPerSecond","Replication Factor");
    @Override
    public void open(MetricConfig config) {

    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        fileHashMap.remove(metric);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        createFileForMetric(metric,metricName, group);
    }

    @Override
    public void close() {
        fileHashMap.clear();
    }

    @Override
    public void report() {
        fileHashMap.forEach((metric, file) -> {
            try{
                if(metric instanceof Gauge){
                    Gauge tmp = (Gauge) metric;
                    Files.write(file.toPath(), (tmp.getValue().toString()+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                }
                if(metric instanceof Counter){
                    Counter tmp = (Counter) metric;
                    Files.write(file.toPath(), (tmp.getCount() +"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                }
                if(metric instanceof Meter){
                    Meter tmp = (Meter) metric;
                    Files.write(file.toPath(), (tmp.getRate() + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public synchronized void createFileForMetric(Metric metric, String metricName, MetricGroup group){
        StringBuilder str = new StringBuilder();
        Map<String, String> variables = group.getAllVariables();
        if(names.contains(metricName) && variables.containsKey("<job_name>") && variables.containsKey("<operator_name>") && variables.containsKey("<subtask_index>")){
            str.append(variables.get("<job_name>"));
            str.append('/');
            str.append(variables.get("<operator_name>"));
            str.append('/');
            str.append(metricName);
            str.append('/');
            str.append(variables.get("<subtask_index>"));
            File file = new File(String.format("%s/metrics/%s", System.getenv("HOME"),str));
            File parent = file.getParentFile();
            try {
                parent.mkdirs();
                file.createNewFile();
                fileHashMap.put(metric, file);
            } catch (IllegalStateException | IOException e) {
                e.printStackTrace();
            }
        }

    }

}
