package datasets;

import elements.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

/**
 * Dataset for signed network where format is assumed to <src, dest, sign ,ts>
 */
public class SignedNetworkDataset implements Dataset {

    public final String fileName;

    public SignedNetworkDataset(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<GraphOp> out;
        TextInputFormat format = new TextInputFormat(new Path(fileName));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");

        if (fineGrainedResourceManagementEnabled) {
//            env.registerSlotSharingGroup(SlotSharingGroup.newBuilder("edge-file").setCpuCores(1).setTaskHeapMemoryMB(10).build());
            out = env.readFile(format, fileName, FileProcessingMode.PROCESS_ONCE, Long.MAX_VALUE).setParallelism(1).slotSharingGroup("edge-file")
                    .map(new Parser()).setParallelism(1).slotSharingGroup("edge-file")
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            long ts = element.getTimestamp();
                            element.setTimestamp(null);
                            return ts;
                        }
                    })).setParallelism(1).slotSharingGroup("edge-file");
        } else {
            out = env.readFile(format, fileName, FileProcessingMode.PROCESS_ONCE, Long.MAX_VALUE).setParallelism(1)
                    .map(new Parser()).setParallelism(1)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            long ts = element.getTimestamp();
                            element.setTimestamp(null);
                            return ts;
                        }
                    })).setParallelism(1);
        }
        return out;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                System.out.println(value);
            }
        };
    }

    public static class Parser implements MapFunction<String, GraphOp>{
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            Edge e = new Edge(new Vertex(values[0]), new Vertex(values[1]));
            e.setFeature("sign", new Feature<>(Integer.valueOf(values[2])));
            return new GraphOp(Op.COMMIT,e,Float.valueOf(values[3]).longValue());
        }
    }

}
