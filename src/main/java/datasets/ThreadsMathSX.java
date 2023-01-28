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
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ThreadsMathSX extends Dataset {

    /**
     * Type of dataset to be used
     * <p>
     * t2q -> Tag to question: Meaning 1 Tag with a list of Question
     * t2q -> Tag to question: Meaning 1 Question with a list of Tags
     * </p>
     */
    @CommandLine.Option(names = {"--threadsMathSX:datasetType"}, defaultValue = "t2q", fallbackValue = "t2q", arity = "1", description = {"Type of tags dataset: q2t (Question to tag) or t2q (Tag to question)"})
    protected String datasetType;

    /**
     * Type of the stream:
     * <p>
     * hypergraph -> Producing {@link HyperEgoGraph}s
     * edge-stream -> Producing a stream of {@link DirectedEdge}s
     * </p>
     */
    @CommandLine.Option(names = {"--threadsMathSX:streamType"}, defaultValue = "hypergraph", fallbackValue = "hypergraph", arity = "1", description = {"Type of stream: edge-stream or hypergraph"})
    protected String streamType;

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String fileName = Path.of(System.getenv("DATASET_DIR"), "threads-math-sx", datasetType.equals("t2q") ? "threads-math-sx[tag-question].txt" : "threads-math-sx[question-tag].txt").toString();
        String opName = String.format("ThreadsMathSX[dataset=%s, stream=%s]", datasetType, streamType);
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(opName).setParallelism(1);
        return (streamType.equals("hypergraph") ? fileReader.flatMap(new ParseHyperGraph()) : fileReader.flatMap(new ParseEdges())).setParallelism(1).name(String.format("Parser %s", opName));
    }

    @Override
    public boolean isResponsibleFor(String datasetName) {
        return datasetName.equals("threads-math-sx");
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
                ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
            }
        };
    }


    /**
     * String -> {@link GraphOp} for star-graph stream
     */
    protected static class ParseEdges implements FlatMapFunction<String, GraphOp> {
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
    protected static class ParseHyperGraph implements FlatMapFunction<String, GraphOp> {
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
