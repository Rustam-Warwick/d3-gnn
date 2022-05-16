package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

public interface Dataset {
    OutputTag<GraphOp> TRAIN_TEST_DATA_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class));

    /**
     * Build the stream of dataset, returns array of output streams
     */
    DataStream<GraphOp>[] build(StreamExecutionEnvironment env);

    /**
     * Split the label into testLabel and trainLabel
     *
     * @return normal stream, but the label stream should be splitted into {@link{Dataset.TRAIN_TEST_DATA_OUTPUT}
     */
    ProcessFunction<GraphOp, GraphOp> trainTestSplitter();
}
