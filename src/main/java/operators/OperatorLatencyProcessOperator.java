package operators;

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.LatencyStats;

public class OperatorLatencyProcessOperator<K, IN1, IN2, OUT> extends KeyedCoProcessOperator<K, IN1, IN2, OUT> {

    public OperatorLatencyProcessOperator(KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction) {
        super(keyedCoProcessFunction);
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        int historySize = getOperatorConfig().getConfiguration().getInteger(MetricOptions.LATENCY_HISTORY_SIZE);
        this.latencyStats =
                new LatencyStats(
                        metrics.addGroup("nativeLatency"),
                        historySize,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getOperatorID(),
                        LatencyStats.Granularity.OPERATOR);
    }

}
