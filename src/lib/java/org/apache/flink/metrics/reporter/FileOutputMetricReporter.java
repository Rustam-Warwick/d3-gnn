package org.apache.flink.metrics.reporter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.*;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Metric Reporter that ouputs to a file all the task metrics
 */
public class FileOutputMetricReporter implements Scheduled, MetricReporter, MetricReporterFactory
{
    private final String BASE_PATH;
    protected final Map<String, Tuple2<BufferedWriter, Metric>> metricsMap = new HashMap<>(100);


    public FileOutputMetricReporter(){
        this.BASE_PATH = System.getenv("HOME");
    }

    public FileOutputMetricReporter(String BASE_PATH) {
        this.BASE_PATH = BASE_PATH;
    }

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new FileOutputMetricReporter(properties.getProperty("base_path", BASE_PATH));
    }

    @Override
    public void open(MetricConfig config) {
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        String fileName = getMetricFileName(metric, metricName, group);
        if (fileName != null) {
            Tuple2<BufferedWriter, Metric> tmp = null;
            synchronized (this) {
                tmp = metricsMap.remove(fileName);
            }
            if (tmp != null) {
                try {
                    tmp.f0.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    synchronized public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String fileName = getMetricFileName(metric, metricName, group);
        if (fileName != null && !metricsMap.containsKey(fileName)){
            try {
                File file = new File(String.format("%s/metrics/%s",BASE_PATH, fileName));
                File parent = file.getParentFile();
                parent.mkdirs();
                file.createNewFile();
                metricsMap.put(fileName, new Tuple2<>( new BufferedWriter(new FileWriter(file,true)), metric));
            } catch (IllegalStateException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    synchronized public void close() {
        metricsMap.values().forEach(item -> {
            try {
                item.f0.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        metricsMap.clear();
    }

    @Override
    synchronized public void report() {
        metricsMap.forEach((fileName, values) -> {
            try {
                if (values.f1 instanceof Gauge) {
                    Gauge tmp = (Gauge) values.f1;
                    values.f0.write(tmp.getValue().toString());
                } else if (values.f1 instanceof Counter) {
                    Counter tmp = (Counter) values.f1;
                    values.f0.write(Long.toString(tmp.getCount()));
                } else if (values.f1 instanceof Meter) {
                    Meter tmp = (Meter) values.f1;
                    values.f0.write(Double.toString(tmp.getRate()));
                }
                values.f0.newLine();
            }catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Nullable
    protected String getMetricFileName(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> variables = group.getAllVariables();
        if(!variables.containsKey("<job_name>")) return null;
        String prefix = variables.get("<job_name>") + "/";
        prefix += variables.containsKey("<tm_id>") ? variables.get("<tm_id>")+"/": "";
        prefix += variables.containsKey("<task_name>") ? "[TASK]"+variables.get("<task_name>")+"/" : "";
        prefix += variables.containsKey("<operator_name>") ? "[OP]"+variables.get("<operator_name>")+"/" : "";
        prefix += metricName;
        prefix += variables.containsKey("<subtask_index>") ? "/"+variables.get("<subtask_index>") : "";
        return prefix;
    }
}
