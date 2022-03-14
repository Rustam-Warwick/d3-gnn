package functions;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import elements.ElementType;
import elements.GraphElement;
import elements.GraphOp;
import elements.Op;
import features.Tensor;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.configuration.Configuration;


/**
 * Function that takes in embeddigs and features and produces loss and
 */
public class GraphLossFn extends RichJoinFunction<GraphOp, GraphOp, GraphOp> {
    public Loss loss = null;

    public Loss lossFunction(){
        return new SoftmaxCrossEntropyLoss();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.loss = lossFunction();
    }

    @Override
    public GraphOp join(GraphOp first, GraphOp second) throws Exception {
        NDManager manager = NDManager.newBaseManager();
        GradientCollector collector = manager.getEngine().newGradientCollector();
        NDArray logit = ((NDArray) first.element.getFeature("logits").getValue()).expandDims(0);
        logit.setRequiresGradient(true);
        Integer tmp = (Integer) second.element.getFeature("label").getValue();
        NDArray label = manager.create(tmp).expandDims(0);
        NDArray loss = this.loss.evaluate(new NDList(label), new NDList(logit));
        collector.backward(loss);
        collector.close();
        manager.close();
        GraphElement el = first.element.copy();
        el.setFeature("grad",new Tensor(logit.getGradient()));
        Rpc backward = new Rpc("trainer", "backward", new Object[]{el.getFeature("grad")}, ElementType.PLUGIN, false);
        return new GraphOp(Op.RPC, first.part_id, backward, IterationState.BACKWARD);
    }
}
