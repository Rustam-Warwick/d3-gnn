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
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

import java.nio.file.Path;


/**
 * Subreddit -> Subreddit networks in Reddit Social Network
 * <a href="https://snap.stanford.edu/data/soc-RedditHyperlinks.html">link</a>
 */
public class RedditHyperlink extends Dataset {

    /**
     * Type of reddit hyperlink stream: full, body, title
     */
    @CommandLine.Option(names = {"--redditHyperlink:type"}, defaultValue = "body", fallbackValue = "body", arity = "1", description = {"Type of reddit hyperlink: body, title, full"})
    protected String type;

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String topologyFileName = Path.of(System.getenv("DATASET_DIR"), "RedditHyperlinks", String.format("soc-redditHyperlinks-%s.tsv", type)).toString();
        String topologyOperatorName = String.format("Reddit Hyperlink[%s]", type);
        SingleOutputStreamOperator<String> topologyFileStream = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(topologyFileName)), topologyFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name(topologyOperatorName).setParallelism(1);
        return topologyFileStream.map(new TopologyParser()).name(String.format("Parser %s", topologyOperatorName)).setParallelism(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new TrainTestSplitter();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isResponsibleFor(String datasetName) {
        return datasetName.equals("reddit-hyperlink");
    }

    /**
     * Actual Splitter function
     */
    protected static class TrainTestSplitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            out.collect(value);
            ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value); // Edge with Features even for the topology
        }
    }

    /**
     * Parser for the Stream topology
     */
    protected static class TopologyParser implements MapFunction<String, GraphOp> {

        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split("\t");
            DirectedEdge directedEdge = new DirectedEdge(new Vertex(values[0]), new Vertex(values[1]));
            return new GraphOp(Op.ADD, directedEdge);
        }

    }

}
