package datasets;

import elements.DEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.Op;
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
 * Expecting an edge-list files, (index, src, dest)
 */
public class EdgeList implements Dataset {
    protected String edgeFileName;

    public EdgeList(String edgeFileName) {
        this.edgeFileName = edgeFileName;
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<GraphOp> out;
        TextInputFormat format = new TextInputFormat(new Path(edgeFileName));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");

        if (fineGrainedResourceManagementEnabled) {
//            env.registerSlotSharingGroup(SlotSharingGroup.newBuilder("edge-file").setCpuCores(1).setTaskHeapMemoryMB(10).build());
            out = env.readFile(format, edgeFileName, FileProcessingMode.PROCESS_ONCE, Long.MAX_VALUE).setParallelism(1).slotSharingGroup("edge-file")
                    .map(new EdgeParser()).setParallelism(1).slotSharingGroup("edge-file")
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            return element.getTimestamp();
                        }
                    })).setParallelism(1).slotSharingGroup("edge-file");
        } else {
            out = env.readFile(format, edgeFileName, FileProcessingMode.PROCESS_ONCE, Long.MAX_VALUE).setParallelism(1)
                    .map(new EdgeParser()).setParallelism(1)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            return element.getTimestamp();
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
                assert value.element.elementType() == ElementType.EDGE;
                GraphOp copy = value.copy();
                copy.setElement(value.element.copy(CopyContext.MEMENTO));
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, copy);
                out.collect(value);
            }
        };
    }

    protected static class EdgeParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] edges = value.split(",");
            DEdge e = new DEdge(new Vertex(edges[1]), new Vertex(edges[2]));
            long ts = Long.parseLong(edges[0]);
            return new GraphOp(Op.COMMIT, e);
        }
    }
}
