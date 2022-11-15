package datasets;

import elements.*;
import elements.enums.Op;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CoAuthDBLPEStream implements Dataset {
    private final String vertexStreamFile;

    public CoAuthDBLPEStream(String datasetDir) {
        vertexStreamFile = Path.of(datasetDir, "coauth-DBLP-full", "coauth-DBLP-vertex-stream.txt").toString();
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        SingleOutputStreamOperator<String> vertexStreamString = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(vertexStreamFile)), vertexStreamFile, FileProcessingMode.PROCESS_CONTINUOUSLY, 100000).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> edges = vertexStreamString.flatMap(new ParseEdgeStream()).setParallelism(1);
        if (fineGrainedResourceManagementEnabled) {
            // All belong to the same slot sharing group
            vertexStreamString.slotSharingGroup("file-input");
            edges.slotSharingGroup("file-input");
        }
        return edges;
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

    public static class ParseEdgeStream implements FlatMapFunction<String, GraphOp> {
        int count;
        @Override
        public void flatMap(String value, Collector<GraphOp> out){
            if(++count > 1000) {
                return;
            }
            String[] values = value.split(",");
            Vertex src = new Vertex(values[0]);
            for (int i = 1; i < values.length; i++) {
                Vertex dest = new Vertex(values[i]);
                out.collect(new GraphOp(Op.COMMIT, new DEdge(src, dest)));
                out.collect(new GraphOp(Op.COMMIT, new DEdge(dest,src)));
            }
        }
    }
}
