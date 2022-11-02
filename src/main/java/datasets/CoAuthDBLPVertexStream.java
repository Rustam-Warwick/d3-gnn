package datasets;

import elements.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CoAuthDBLPVertexStream implements Dataset {
    private final String vertexStreamFile;

    public CoAuthDBLPVertexStream(String datasetDir) {
        vertexStreamFile = Path.of(datasetDir, "coauth-DBLP-full", "coauth-DBLP-vertex-stream.txt").toString();
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        SingleOutputStreamOperator<String> vertexStreamString = env.readTextFile(vertexStreamFile).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> nets = vertexStreamString.map(new ParseVertexStream());
        if (fineGrainedResourceManagementEnabled) {
            // All belong to the same slot sharing group
            vertexStreamString.slotSharingGroup("file-input");
            nets.slotSharingGroup("file-input");
        }
        return nets;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                out.collect(value);
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, value);
            }
        };
    }

    public static class ParseVertexStream extends RichMapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            List<Vertex> src = List.of(new Vertex(values[0]));
            List<String> srcId = List.of(src.get(0).getId());
            List<HEdge> hEdges = new ArrayList<>(values.length - 1);
            for (int i = 1; i < values.length; i++) {
                String netId = values[i];
                hEdges.add(new HEdge(netId, srcId, false, (short) -1));
            }
            HGraph hGraph = new HGraph(src, hEdges);
            return new GraphOp(Op.COMMIT, hGraph);
        }
    }
}
