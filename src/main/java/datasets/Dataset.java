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


    static Dataset getDataset(String name) {
        if (name.equals("reddit-hyperlink")) {
            return new RedditHyperlink(Path.of(System.getenv("DATASET_DIR")).toString());
        }
        if (name.equals("stackoverflow")) {
            return new Stackoverflow(Path.of(System.getenv("DATASET_DIR")).toString());
        } else if (name.contains("signed")) {
            return new SignedNetworkDataset(Path.of(System.getenv("DATASET_DIR"), name).toString());
        } else {
            return new EdgeList(Path.of(System.getenv("DATASET_DIR"), "edge-list", name).toString());
        }
    }

    /**
     * Build the stream of dataset, returns array of output streams
     */
    DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled);

    /**
     * Split the label into testLabel and trainLabel
     *
     * @return normal stream, but the label stream should be splitted into {@link{Dataset.TRAIN_TEST_DATA_OUTPUT}
     */
    KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter();
}
