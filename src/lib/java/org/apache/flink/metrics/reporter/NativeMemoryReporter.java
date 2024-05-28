package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.shaded.zookeeper3.com.codahale.metrics.Gauge;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NativeMemoryReporter extends AbstractReporter implements Scheduled, MetricReporterFactory{
    private final Map<String, Long> memoryMetrics = new HashMap<>();
    private TaskManagerMetricGroup tmMetricGroup;

    @Override
    public String filterCharacters(String s) {
        return null;
    }

    @Override
    public void open(MetricConfig metricConfig) {
//        this.tmMetricGroup = (TaskManagerMetricGroup) metricConfig.getScopeFormats();
        registerNativeMemoryMetrics();
    }
    private void registerNativeMemoryMetrics() {
        tmMetricGroup.gauge("NativeMemory.Total",  () -> memoryMetrics.getOrDefault("Total", 0L));
        tmMetricGroup.gauge("NativeMemory.Heap",  () -> memoryMetrics.getOrDefault("Java Heap", 0L));
        tmMetricGroup.gauge("NativeMemory.Class",  () -> memoryMetrics.getOrDefault("Class", 0L));
        tmMetricGroup.gauge("NativeMemory.Thread",  () -> memoryMetrics.getOrDefault("Thread", 0L));
        tmMetricGroup.gauge("NativeMemory.Code", () -> memoryMetrics.getOrDefault("Code", 0L));
        tmMetricGroup.gauge("NativeMemory.GC", () -> memoryMetrics.getOrDefault("GC", 0L));
        tmMetricGroup.gauge("NativeMemory.Compiler", () -> memoryMetrics.getOrDefault("Compiler", 0L));
        tmMetricGroup.gauge("NativeMemory.Internal",  () -> memoryMetrics.getOrDefault("Internal", 0L));
        tmMetricGroup.gauge("NativeMemory.Symbol",  () -> memoryMetrics.getOrDefault("Symbol", 0L));
        tmMetricGroup.gauge("NativeMemory.NMT",  () -> memoryMetrics.getOrDefault("Native Memory Tracking", 0L));
        tmMetricGroup.gauge("NativeMemory.SharedClassSpace", () -> memoryMetrics.getOrDefault("Shared class space", 0L));
        tmMetricGroup.gauge("NativeMemory.Unknown",  () -> memoryMetrics.getOrDefault("Unknown", 0L));
    }
    @Override
    public void close() {

    }

    private String getProcessId() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return processName.split("@")[0];
    }

    private void parseMemoryLine(String line) {
        if (line.contains("reserved=")) {
            String[] parts = line.split("\\s+");
            String category = parts[0];
            long committed = Long.parseLong(parts[3].replaceAll("[^0-9]", ""));
            memoryMetrics.put(category, committed);
        }
    }

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new NativeMemoryReporter();
    }

    @Override
    public void report()  {
        try {
            Process process = Runtime.getRuntime().exec("jcmd " + getProcessId() + " VM.native_memory summary");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                parseMemoryLine(line);
            }
            reader.close();
            process.waitFor();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
