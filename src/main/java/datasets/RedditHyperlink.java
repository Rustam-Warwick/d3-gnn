package datasets;

import elements.DEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.nio.file.Path;

public class RedditHyperlink implements Dataset {
    private final transient String baseDirectory;

    public RedditHyperlink(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        String fileName = Path.of(baseDirectory, "RedditHyperlinks", "soc-redditHyperlinks-body.tsv").toString();
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, FileProcessingMode.PROCESS_CONTINUOUSLY, 100000).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = fileReader.map(new Parser()).setParallelism(1).filter(new FilterFunction<GraphOp>() {
            int counter;

            @Override
            public boolean filter(GraphOp value) throws Exception {
                return ++counter <= 10000;
            }
        }).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> timestampExtracted = parsed.assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
            @Override
            public long extractTimestamp(GraphOp element, long recordTimestamp) {
                Long ts = element.getTimestamp();
                element.setTimestamp(null);
                return ts == null ? System.currentTimeMillis() : ts;
            }
        })).setParallelism(1);
        if (fineGrainedResourceManagementEnabled) {
            // All belong to the same slot sharing group
            fileReader.slotSharingGroup("file-input");
            parsed.slotSharingGroup("file-input");
            timestampExtracted.slotSharingGroup("file-input");
        }
        return timestampExtracted;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new TrainTestSplitter();
    }

    static class TrainTestSplitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            out.collect(value);
            ctx.output(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT, value); // Edge with Features even for the topology
        }
    }

    static class Parser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split("\t");
            DEdge dEdge = new DEdge(new Vertex(values[0]), new Vertex(values[1]), values[2]); // Attributed edges
//            float[] features = new float[values.length - 4];
//            for(int i=4;i<values.length;i++){
//                String processed = values[i].replaceAll("[^0-9.]", "");
//                features[i-4] =  Float.valueOf(processed);
//            }
//            edge.setFeature("f", new Tensor(BaseNDManager.getManager().create(features)));
            return new GraphOp(Op.COMMIT, dEdge);
        }
    }

}
