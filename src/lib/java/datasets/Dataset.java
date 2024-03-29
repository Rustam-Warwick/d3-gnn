package datasets;

import elements.GraphOp;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

import java.io.Serializable;
import java.util.ServiceLoader;

/**
 * Wrapper around graph data that gets streamed in
 * Uses {@link java.util.ServiceLoader} interface
 * <strong> Can be a static file or other dynamic data source </strong>
 */
public abstract class Dataset implements Serializable {

    /**
     * The {@link org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo.ProcessingMode} of the dataset if it is relevant
     */
    @CommandLine.Option(names = {"--dataset:processOnce"}, defaultValue = "true", fallbackValue = "true", arity = "1", description = "Dataset: Process once or continuously")
    protected boolean processOnce;

    /**
     * Is fine grained resource management enabled
     */
    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled;

    /**
     * Helper method for getting the required dataset from string name
     */
    @Nullable
    public static Dataset getDataset(String name, String[] cmdArgs) {
        ServiceLoader<Dataset> partitionerServiceLoader = ServiceLoader.load(Dataset.class);
        for (Dataset dataset : partitionerServiceLoader) {
            if (dataset.isResponsibleFor(name)) {
                dataset.parseCmdArgs(cmdArgs);
                return dataset;
            }
        }
        return null;
    }

    /**
     * Process command line arguments.
     * Made final since base class already has cmd arguments
     */
    public void parseCmdArgs(String[] cmdArgs) {
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Return true is this Dataset object can process the given name
     */
    public abstract boolean isResponsibleFor(String datasetName);

    /**
     * Build the stream of GraphOps
     */
    public abstract DataStream<GraphOp> build(StreamExecutionEnvironment env);

    /**
     * Return the splitter function for processing
     */
    public abstract KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter();

}
