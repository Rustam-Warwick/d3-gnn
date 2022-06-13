package datasets;

import elements.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

public class EdgeList implements Dataset {
    protected String edgeFileName;

    public EdgeList(String edgeFileName) {
        this.edgeFileName = edgeFileName;
    }

    @Override
    public DataStream<GraphOp>[] build(StreamExecutionEnvironment env) {
        return new DataStream[]{
                env.readTextFile(edgeFileName)
                        .map(new EdgeParser())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
                    @Override
                    public long extractTimestamp(GraphOp element, long recordTimestamp) {
                        return ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE - 1);
                    }
                }))

        };
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
            Edge e = new Edge(new Vertex(edges[0]), new Vertex(edges[1]));
            return new GraphOp(Op.COMMIT, e, null);
        }
    }
}
