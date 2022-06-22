package datasets;

import elements.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
    public DataStream<GraphOp>[] build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<GraphOp> out;
        if (fineGrainedResourceManagementEnabled) {
            env.registerSlotSharingGroup(SlotSharingGroup.newBuilder("edge-file").setCpuCores(1).setTaskHeapMemoryMB(10).build());
            out = env.readTextFile(edgeFileName).setParallelism(1).slotSharingGroup("edge-file")
                    .map(new EdgeParser()).setParallelism(1).slotSharingGroup("edge-file")
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            return element.getTimestamp();
                        }
                    })).setParallelism(1).slotSharingGroup("edge-file");
        } else {
            out = env.readTextFile(edgeFileName).setParallelism(1)
                    .map(new EdgeParser()).setParallelism(1)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                        @Override
                        public long extractTimestamp(GraphOp element, long recordTimestamp) {
                            return element.getTimestamp();
                        }
                    })).setParallelism(1);
        }
        return new DataStream[]{out};
    }

    @Override
    public KeyedProcessFunction<String, GraphOp, GraphOp> trainTestSplitter() {
        return new KeyedProcessFunction<String, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                assert value.element.elementType() == ElementType.EDGE;
                GraphOp copy = value.copy();
                copy.setElement(value.element.copy());
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, copy);
                out.collect(value);
            }
        };
    }

    protected static class EdgeParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] edges = value.split(",");
            Edge e = new Edge(new Vertex(edges[1]), new Vertex(edges[2]));
            long ts = Long.parseLong(edges[0]);
            return new GraphOp(Op.COMMIT, e, ts);
        }
    }
}
