package datasets;

import elements.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Path;

public class CoauthDBLPVertexStream implements Dataset {
    final String vertexStreamFile;

    public CoauthDBLPVertexStream(String datasetDir) {
        vertexStreamFile = Path.of(datasetDir, "coauth-DBLP-vertex-stream.txt").toString();
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<String> vertexStreamString = env.readTextFile(vertexStreamFile.toString()).setParallelism(1);
        DataStream<GraphOp> nets = vertexStreamString.map(new ParseVertexStream());
        return nets;
    }

   public static class ParseVertexStream extends RichMapFunction<String, GraphOp>{
       @Override
       public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            Vertex[] src = new Vertex[]{new Vertex(values[0])}; // Center of the vertex
            HEdge[] hEdges = new HEdge[values.length - 1];
            for (int i = 1; i < values.length; i++) {
                String netId = values[i];
                hEdges[i-1] = new HEdge(netId,src);
            }
            HGraph hGraph = new HGraph(src, hEdges);
            GraphOp op = new GraphOp(Op.COMMIT, hGraph);
            return op;
       }
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
}
