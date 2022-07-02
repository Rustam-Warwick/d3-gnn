package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FileOutputMetricReporter extends AbstractReporter implements  Scheduled {
    public HashMap<Metric, File> fileHashMap;

    @Override
    public String filterCharacters(String input) {
        return input;
    }

    @Override
    public void open(MetricConfig config) {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        super.notifyOfAddedMetric(metric, metricName, group);
        createFileForMetric(metricName, group);
    }

    @Override
    public void close() {

    }

    @Override
    public void report() {
    }

    public void createFileForMetric(String metricName, MetricGroup group){
        StringBuilder str = new StringBuilder();
        Map<String, String> variables = group.getAllVariables();
        if(variables.containsKey("job_name")){
            System.out.println(metricName);
            str.append(group.getAllVariables().get("job_name"));
        }

    }

}
