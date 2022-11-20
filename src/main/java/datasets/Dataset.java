package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import picocli.CommandLine;

import javax.annotation.Nullable;
import java.io.Serializable;

public abstract class Dataset implements Serializable {
    public static OutputTag<GraphOp> TRAIN_TEST_SPLIT_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class)); // Output of train test split data

    public static OutputTag<GraphOp> TOPOLOGY_ONLY_DATA_OUTPUT = new OutputTag<>("topologyData", TypeInformation.of(GraphOp.class)); // Output that only contains the topology of the graph updates

    @CommandLine.Option(names = {"--dataset:processOnce"}, defaultValue = "true", fallbackValue = "true", arity = "1", description = "Dataset: Process once or continuously")
    protected boolean processOnce;

    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled; // Add custom slotSharingGroupsForOperators

    public Dataset(String[] cmdArgs) {
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Helper method for getting the required dataset
    */
    @Nullable
    public static Dataset getDataset(String name, String[] cmdArgs) {
        switch (name) {
            case "reddit-hyperlink":
                return new RedditHyperlink(cmdArgs);
            case "tags-ask-ubuntu":
                return new TagsAskUbuntu(cmdArgs);
            default:
                return null;
        }
    }

    /**
     * Build the stream of GraphOp given the dataset and fineGrainedResourceManagement
     */
    public abstract DataStream<GraphOp> build(StreamExecutionEnvironment env);

    /**
     * Return process function to split the label into train-test, topology and normal stream
     */
    public abstract KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter();

}
