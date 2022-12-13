package helpers.datasets;

import datasets.Dataset;
import elements.GraphOp;
import elements.HyperEgoGraph;
import elements.Vertex;
import elements.enums.Op;
import helpers.utils.RandomStringGenerator;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link Dataset} Generating a Mesh HyperGraph as a stream of {@link elements.HyperEgoGraph}
 */
public class MeshHyperGraphGenerator extends Dataset {

    final int nVertices;

    public MeshHyperGraphGenerator(int nVertices) {
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
        GraphOp[] resultingEdges = new GraphOp[nVertices];
        List<String> hyperEdges = IntStream.range(0, nVertices).mapToObj(item -> RandomStringGenerator.getRandomString(20)).collect(Collectors.toList());
        for (int i = 0; i < nVertices; i++) {
            resultingEdges[i] = new GraphOp(Op.COMMIT,
                    new HyperEgoGraph(new Vertex(String.valueOf(i)), hyperEdges));
        }
        return List.of(resultingEdges);
    }

}
