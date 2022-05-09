package datasets;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Dataset {
    DataStream<GraphOp> build(StreamExecutionEnvironment env);
}
