package helpers.datasets;

import datasets.Dataset;
import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.List;

/**
 * {@link Dataset} Generating a Mesh Graph as a stream of {@link elements.DirectedEdge}
 */
public class MeshGraphGenerator extends Dataset {

    final int nVertices;

    public MeshGraphGenerator(int nVertices) {
        Preconditions.checkState(nVertices > 1);
        this.nVertices = nVertices;
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        DataStreamSource<GraphOp> stream = env.fromCollection(generateEdges());
        if (fineGrainedResourceManagementEnabled) stream.slotSharingGroup("file-input");
        return stream;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                out.collect(value);
                ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
            }
        };
    }


    protected Collection<GraphOp> generateEdges() {
        int meshEdgeSize = nVertices * (nVertices - 1);
        GraphOp[] resultingEdges = new GraphOp[meshEdgeSize];
        for (int i = 0; i < nVertices; i++) {
            for (int j = 0; j < nVertices; j++) {
                if (i == j) continue;
                GraphOp thisEdge = new GraphOp(Op.COMMIT, new DirectedEdge(new Vertex(String.valueOf(i)), new Vertex(String.valueOf(j))));
                int startIndex = Math.abs(thisEdge.hashCode()) % meshEdgeSize;
                while (true) {
                    if (resultingEdges[startIndex] == null) {
                        resultingEdges[startIndex] = thisEdge;
                        break;
                    }
                    startIndex = (startIndex + 1) % meshEdgeSize;
                }
            }
        }
        return List.of(resultingEdges);
    }

}
