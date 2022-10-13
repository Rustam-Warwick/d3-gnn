package datasets;

import elements.GraphOp;
import elements.HEdge;
import elements.Op;
import elements.Vertex;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CoauthDBLP implements Dataset {
    final String timestampFile;
    final String simplicesFile;
    final String nVertexFile;

    public CoauthDBLP(String datasetDir) {
        timestampFile = Path.of(datasetDir, "coauth-DBLP-full-times.txt").toString();
        simplicesFile = Path.of(datasetDir, "coauth-DBLP-full-simplices.txt").toString();
        nVertexFile = Path.of(datasetDir, "coauth-DBLP-full-nverts.txt").toString();
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<String> timestampStream = env.readTextFile(timestampFile).setParallelism(1);
        DataStream<String> simplicesStream = env.readTextFile(simplicesFile).setParallelism(1);
        DataStream<String> nVertexStream = env.readTextFile(nVertexFile).setParallelism(1);
        DataStream<GraphOp> nets = simplicesStream.connect(nVertexStream).process(new CoProcessNets());
        return nets;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return null;
    }

    public static class CoProcessNets extends CoProcessFunction<String, String, GraphOp> {
        List<Integer> nVertices = new ArrayList<>();
        List<String> vertices = new ArrayList<>();
        Integer verticesCount = 0;

        @Override
        public void processElement1(String value, CoProcessFunction<String, String, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            // Simplex Stream
            vertices.add(value);
            if (nVertices.size() > 0 && vertices.size() >= nVertices.get(0)) {
                List<Vertex> thisNetVertices = new ArrayList<>();
                for (int i = 0; i < nVertices.get(0); i++) {
                    String tmp = vertices.remove(0);
                    thisNetVertices.add(new Vertex(tmp));
                }
                HEdge myHEdge = null;
                out.collect(new GraphOp(Op.COMMIT, myHEdge));
                nVertices.remove(0);
            }
        }

        @Override
        public void processElement2(String value, CoProcessFunction<String, String, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            // NVertex Stream
            nVertices.add(Integer.parseInt(value));
        }
    }
}
