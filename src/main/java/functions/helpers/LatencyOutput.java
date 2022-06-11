package functions.helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LatencyOutput extends CoProcessFunction<GraphOp, GraphOp, Void> {
    private final HashMap<Long, List<Long>> requestLatencies = new HashMap<>();
    private final String outputFileName;

    public LatencyOutput(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    @Override
    public void close() throws Exception {
        super.close();
        String homePath = System.getenv("HOME");
        File outputFile = new File(String.format("%s/metrics/%s/latencies.csv", homePath, outputFileName));
        File parent = outputFile.getParentFile();
        try {
            parent.mkdirs();
            outputFile.createNewFile();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }

        StringBuilder builder = new StringBuilder();
        requestLatencies.forEach((timestamp, processingTimeLists)->{
            for (Long processingTime : processingTimeLists) {
                builder.append(String.format("%s,%s\n",timestamp, processingTime ));
            }
        });

        Files.write(outputFile.toPath(), builder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }

    /**
     * Input data stream
     */
    @Override
    public void processElement1(GraphOp value, CoProcessFunction<GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
//        commitIfWatermarkChanged(ctx.timerService().currentWatermark());
        ArrayList<Long> processingTimes = new ArrayList<>();
        processingTimes.add(ctx.timerService().currentProcessingTime());
        requestLatencies.put(ctx.timestamp(), processingTimes);
    }

    /**
     * Output Data Stream
     */
    @Override
    public void processElement2(GraphOp value, CoProcessFunction<GraphOp, GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
//        commitIfWatermarkChanged(ctx.timerService().currentWatermark());
        requestLatencies.computeIfPresent(ctx.timestamp(), (key, val)->{
            val.add(ctx.timerService().currentProcessingTime());
            return val;
        });
    }
}
