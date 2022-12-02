package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

import java.io.Serializable;

/**
 * Wrapper around graph data that gets streamed in
 * Can be a static file or other dynamic data source
 */
public abstract class Dataset implements Serializable {

    /**
     * OutputTag for <strong>train</strong> and <strong>test</strong> data dedicated for the last storage layer
     */
    public static OutputTag<GraphOp> TRAIN_TEST_SPLIT_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class)); // Output of train test split data

    /**
     * OutputTag for <strong>topology only data</strong> data dedicated for mid storage layers
     */
    public static OutputTag<GraphOp> TOPOLOGY_ONLY_DATA_OUTPUT = new OutputTag<>("topologyData", TypeInformation.of(GraphOp.class)); // Output that only contains the topology of the graph updates

    /**
     *
     */
    @CommandLine.Option(names = {"--dataset:processOnce"}, defaultValue = "true", fallbackValue = "true", arity = "1", description = "Dataset: Process once or continuously")
    protected boolean processOnce;

    /**
     * Is fine grained resource management enabled
     */
    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled; // Add custom slotSharingGroupsForOperators

    public Dataset() {

    }

    public Dataset(String[] cmdArgs) {
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Helper method for getting the required dataset from string name
     */
    @Nullable
    public static Dataset getDataset(String name, String[] cmdArgs) {
        switch (name) {
            case "reddit-hyperlink":
                return new RedditHyperlink(cmdArgs);
            case "tags-ask-ubuntu":
                return new TagsAskUbuntu(cmdArgs);
            case "ogb-products":
                return new OGBProducts(cmdArgs);
            case "coauth-DBLP":
                return new CoAuthDBLP(cmdArgs);
            case "threads-math-sx":
                return new ThreadsMathSX(cmdArgs);
            default:
                return null;
        }
    }

    /**
     * Build the stream of GraphOps
     */
    public abstract DataStream<GraphOp> build(StreamExecutionEnvironment env);

    /**
     * Return the splitter function for processing
     */
    public abstract KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter();

}
