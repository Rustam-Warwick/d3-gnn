package functions;

import ai.djl.ndarray.NDArray;
import ai.djl.training.loss.Loss;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import elements.*;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.Objects;

/**
 * Function that takes in embeddigs and features and produces loss and
 */
public class GraphLossFn extends RichJoinFunction<GraphOp, GraphOp, GraphOp> {
    public Loss loss = new SoftmaxCrossEntropyLoss()
    public NDArray softmax(NDArray X) {
        NDArray Xexp = X.exp();
        NDArray partition = Xexp.sum(new int[]{1}, true);
        return Xexp.div(partition); // The broadcast mechanism is applied here
    }

    @Override
    public GraphOp join(GraphOp first, GraphOp second) throws Exception {
        NDArray logit = (NDArray) first.element.getFeature("logits").getValue();
        NDArray label = (NDArray) first.element.getFeature("label").getValue();

    }
}
