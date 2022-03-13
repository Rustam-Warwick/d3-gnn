package functions;

import ai.djl.ndarray.NDArray;
import elements.*;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.Objects;

/**
 * Function that takes in embeddigs and features and produces loss and
 */
public class GraphLossFn extends RichCoGroupFunction<GraphOp, GraphOp, GraphOp> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public NDArray softmax(NDArray X) {
        NDArray Xexp = X.exp();
        NDArray partition = Xexp.sum(new int[]{1}, true);
        return Xexp.div(partition); // The broadcast mechanism is applied here
    }



    @Override
    public void coGroup(Iterable<GraphOp> predictions, Iterable<GraphOp> labels, Collector<GraphOp> out) throws Exception {

    }
}
