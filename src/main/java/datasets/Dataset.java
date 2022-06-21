package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
        switch (name) {
            case "cora":
                return new CoraFull(Path.of(System.getenv("DATASET_DIR"), "cora"));
            default:
                return new EdgeList(Path.of(System.getenv("DATASET_DIR"), "edge-list", name).toString());
        }
    }

    /**
     * Build the stream of dataset, returns array of output streams
     */
    DataStream<GraphOp>[] build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled);

    /**
     * Split the label into testLabel and trainLabel
     *
     * @return normal stream, but the label stream should be splitted into {@link{Dataset.TRAIN_TEST_DATA_OUTPUT}
     */
    KeyedProcessFunction<String, GraphOp, GraphOp> trainTestSplitter();
}
