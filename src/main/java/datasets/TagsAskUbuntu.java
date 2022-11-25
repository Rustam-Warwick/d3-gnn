package datasets;

import elements.DirectedEdge;
import elements.GraphOp;
import elements.HyperEgoGraph;
import elements.Vertex;
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
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TagsAskUbuntu extends Dataset {

    /**
     * Type of dataset to be used
     * <p>
     * v2n -> Vertex-to-Net: Meaning 1 Vertex(tag) with a list of Nets(questions)
     * n2v -> Net-to-Vertex: Meaning 1 Net(question) with a list of Vertices(tag)
     * </p>
     */
    @CommandLine.Option(names = {"--tagsAskUbuntu:datasetType"}, defaultValue = "v2n", fallbackValue = "v2n", arity = "1", description = {"Type of tags dataset: v2n or n2v"})
    protected String datasetType;

    /**
     * Type of the stream:
     * <p>
     * hypergraph -> Producing {@link HyperEgoGraph}s
     * edge-stream -> Producing a stream of {@link DirectedEdge}s
     * </p>
     */
    @CommandLine.Option(names = {"--tagsAskUbuntu:streamType"}, defaultValue = "hypergraph", fallbackValue = "hypergraph", arity = "1", description = {"Type of stream: edge-stream or hypergraph"})
    protected String streamType;

    public TagsAskUbuntu(String[] cmdArgs) {
        super(cmdArgs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String fileName = Path.of(System.getenv("DATASET_DIR"), "tags-ask-ubuntu", datasetType.equals("v2n") ? "tags-ask-ubuntu-node-simplex.txt" : "tags-ask-ubuntu-simplex-node.txt").toString();
        String opName = String.format("TagsAskUbuntu[dataset=%s, stream=%s]", datasetType, streamType);
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(opName).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = (streamType.equals("hypergraph") ? fileReader.flatMap(new ParseHyperGraph()) : fileReader.flatMap(new ParseEdges())).setParallelism(1).name(String.format("Map %s", opName));
        if (fineGrainedResourceManagementEnabled) {
            // All belong to the same slot sharing group
            fileReader.slotSharingGroup("file-input");
            parsed.slotSharingGroup("file-input");
        }
        return parsed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                out.collect(value);
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, value);
            }
        };
    }

    /**
     * String -> {@link GraphOp} for star-graph stream
     */
    public static class ParseEdges implements FlatMapFunction<String, GraphOp> {
        @Override
        public void flatMap(String value, Collector<GraphOp> out) throws Exception {
            String[] values = value.split(",");
            Vertex src = new Vertex(values[0]);
            for (int i = 1; i < values.length; i++) {
                Vertex dest = new Vertex(values[i]);
                out.collect(new GraphOp(Op.COMMIT, new DirectedEdge(src, dest)));
                out.collect(new GraphOp(Op.COMMIT, new DirectedEdge(dest, src)));
            }
        }
    }

    /**
     * String -> {@link GraphOp} hypergraph
     */
    public static class ParseHyperGraph implements FlatMapFunction<String, GraphOp> {
        @Override
        public void flatMap(String value, Collector<GraphOp> out) {
            String[] values = value.split(",");
            Vertex src = new Vertex(values[0]);
            List<String> hyperEdgeIds = new ArrayList<>(values.length - 1);
            hyperEdgeIds.addAll(Arrays.asList(values).subList(1, values.length));
            HyperEgoGraph hyperEgoGraph = new HyperEgoGraph(src, hyperEdgeIds);
            out.collect(new GraphOp(Op.COMMIT, hyperEgoGraph));
        }
    }
}
