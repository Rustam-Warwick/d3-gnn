package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.nio.file.Path;

public interface Dataset extends Serializable {
    OutputTag<GraphOp> TRAIN_TEST_SPLIT_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class)); // Output of train test split data
    OutputTag<GraphOp> TOPOLOGY_ONLY_DATA_OUTPUT = new OutputTag<>("topologyData", TypeInformation.of(GraphOp.class)); // Output that only contains the topology of the graph updates

    /**
     * Helper method for getting the required dataset
     *
     * @param name name of the dataset.
     * @return Dataset object
     */
    static Dataset getDataset(String name) {
        switch (name) {
            case "reddit-hyperlink":
                return new RedditHyperlink(Path.of(System.getenv("DATASET_DIR")).toString());
            case "reddit":
                return new Reddit(Path.of(System.getenv("DATASET_DIR")).toString());
            case "coauth-DBLP-vertex-stream":
                return new CoAuthDBLPVStream(Path.of(System.getenv("DATASET_DIR")).toString(), CoAuthDBLPVStream.TYPE.HYPERVERTEX_STREAM);
            case "coauth-DBLP-edge-stream":
                return new CoAuthDBLPVStream(Path.of(System.getenv("DATASET_DIR")).toString(), CoAuthDBLPVStream.TYPE.EDGE_STREAM);
            case "tags-ask-ubuntu-hypergraph":
                return new TagsAskUbuntu(Path.of(System.getenv("DATASET_DIR")).toString(), TagsAskUbuntu.TYPE.HYPERGRAPH_STREAM);
            case "tags-ask-ubuntu-star-expansion":
                return new TagsAskUbuntu(Path.of(System.getenv("DATASET_DIR")).toString(), TagsAskUbuntu.TYPE.STAR_EXPANSION_STREAM);
            case "stackoverflow":
                return new Stackoverflow(Path.of(System.getenv("DATASET_DIR")).toString());
            default:
                throw new IllegalStateException("Dataset is not found");
        }
    }

    /**
     * Build the stream of GraphOp given the dataset
     *
     * @param env                                  Environment for sourcing
     * @param fineGrainedResourceManagementEnabled should slotSharingGroups be used or not
     * @return stream
     */
    DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled);

    /**
     * Split the label into train-test, topology and normal stream
     * <p>
     * {@code Dataset.TRAIN_TEST_SPLIT_OUTPUT} for train and test splits
     * {@code Dataset.TOPOLOGY_ONLY_DATA_OUTPUT} for feature-less topology graph stream
     * {@code normal output(forward) goes to the first layer so should have features but not labels}
     * </p>
     *
     * @return {@link KeyedProcessFunction} for splitting this dataset after partitioning
     */
    KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter();

    /**
     * Helper method for parsing partitioner specific command line arguments
     *
     * @param cmdArgs Array of parameter arguments passed
     * @return same partitioner
     */
    default Dataset parseCmdArgs(String[] cmdArgs) {
        return this;
    }

}
