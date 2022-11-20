package datasets;

import elements.DEdge;
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

    @CommandLine.Option(names = {"--redditHyperlink:type"}, defaultValue = "body", fallbackValue = "body", arity = "1", description= {"Type of reddit hyperlink: body, title, full"})
    protected String type;

    public RedditHyperlink(String[] cmdArgs) {
        super(cmdArgs);
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String fileName;
        switch (type){
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
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, processOnce?FileProcessingMode.PROCESS_ONCE:FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce?0:1000).name(opName).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = fileReader.map(new Parser()).name(String.format("Map %s", opName)).setParallelism(1);
        if (fineGrainedResourceManagementEnabled) {
            fileReader.slotSharingGroup("file-input");
            parsed.slotSharingGroup("file-input");
        }
        return parsed;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new TrainTestSplitter();
    }

    static class TrainTestSplitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            out.collect(value);
            ctx.output(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT, value); // Edge with Features even for the topology
        }
    }

    static class Parser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split("\t");
            DEdge dEdge = new DEdge(new Vertex(values[0]), new Vertex(values[1]), values[2]);
            return new GraphOp(Op.COMMIT, dEdge);
        }
    }

}
