package datasets;

import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import org.apache.flink.api.common.functions.MapFunction;
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


public class RedditHyperlink extends Dataset {

    /**
     * Type of reddit hyperlink stream: full, body, title
     */
    @CommandLine.Option(names = {"--redditHyperlink:type"}, defaultValue = "body", fallbackValue = "body", arity = "1", description = {"Type of reddit hyperlink: body, title, full"})
    protected String type;

    @CommandLine.Option(names = {"--redditHyperlink:delimiter"}, defaultValue = "\t", fallbackValue = "\t", arity = "1", description = {"Delimiter to be used in the stream"})
    protected String delimiter;

    public RedditHyperlink(String[] cmdArgs) {
        super(cmdArgs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String fileName;
        switch (type) {
            case "body":
                fileName = Path.of(System.getenv("DATASET_DIR"), "RedditHyperlinks", "soc-redditHyperlinks-body.tsv").toString();
                break;
            case "title":
                fileName = Path.of(System.getenv("DATASET_DIR"), "RedditHyperlinks", "soc-redditHyperlinks-title.tsv").toString();
                break;
            case "full":
                fileName = Path.of(System.getenv("DATASET_DIR"), "RedditHyperlinks", "soc-redditHyperlinks-full.tsv").toString();
                break;
            default:
                throw new IllegalStateException("RedditHyperlink operates in 3 modes: body, title and full");
        }
        String opName = String.format("Reddit Hyperlink[%s]", type);
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(opName).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = fileReader.map(new Parser(delimiter)).name(String.format("Map %s", opName)).setParallelism(1);
        if (fineGrainedResourceManagementEnabled) {
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
        return new TrainTestSplitter();
    }

    /**
     * Actual Splitter function
     */
    static class TrainTestSplitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            out.collect(value);
            ctx.output(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT, value); // Edge with Features even for the topology
        }
    }

    /**
     * String -> {@link GraphOp} mapper
     */
    static class Parser implements MapFunction<String, GraphOp> {
        private final String delimiter;

        public Parser(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(delimiter);
            DirectedEdge directedEdge = new DirectedEdge(new Vertex(values[0]), new Vertex(values[1]), values[2]);
            return new GraphOp(Op.COMMIT, directedEdge);
        }
    }

}
