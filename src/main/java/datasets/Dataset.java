package datasets;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

public interface Dataset {
    OutputTag<GraphOp> TRAIN_TEST_DATA_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class));

    DataStream<GraphOp>[] build(StreamExecutionEnvironment env);

    ProcessFunction<GraphOp, GraphOp> trainTestSplitter();
}
