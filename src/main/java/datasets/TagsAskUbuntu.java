package datasets;

import elements.DirectedEdge;
import elements.EgoHyperGraph;
import elements.GraphOp;
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
     * Type of TagAskUbuntu stream: hypergraph, star-graph stream
     */
    @CommandLine.Option(names = {"--tagsAskUbuntu:type"}, defaultValue = "hypergraph", fallbackValue = "hypergraph", arity = "1", description = {"Type of tags stream: hypergraph or star-graph"})
    protected String type;

    public TagsAskUbuntu(String[] cmdArgs) {
        super(cmdArgs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String fileName;
        switch (type) {
            case "hypergraph":
                fileName = Path.of(System.getenv("DATASET_DIR"), "tags-ask-ubuntu", "tags-ask-ubuntu-node-simplex.txt").toString();
                break;
            case "star-graph":
                fileName = Path.of(System.getenv("DATASET_DIR"), "tags-ask-ubuntu", "tags-ask-ubuntu-simplex-node.txt").toString();
                break;
            default:
                throw new IllegalStateException("TagsAskUbuntu operates in 2 modes: hypergraph or star-graph");
        }
        String opName = String.format("TagsAskUbuntu[%s]", type);
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(opName).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = (type.equals("hypergraph") ? fileReader.flatMap(new ParseHyperGraph()) : fileReader.flatMap(new ParseGraph())).setParallelism(1).name(String.format("Map %s", opName));
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
    public static class ParseGraph implements FlatMapFunction<String, GraphOp> {
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
            EgoHyperGraph egoHyperGraph = new EgoHyperGraph(src, hyperEdgeIds);
            out.collect(new GraphOp(Op.COMMIT, egoHyperGraph));
        }
    }
}
