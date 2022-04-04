package functions;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import elements.ElementType;
import elements.GraphOp;
import elements.Op;
import features.VTensor;
import helpers.TaskNDManager;
import iterations.IterationType;
import iterations.Rmi;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import scala.Tuple2;

import java.io.IOException;


/**
 * Function that takes in embeddigs and features and produces loss and
 */
public class GraphLossFn extends RichJoinFunction<GraphOp, GraphOp, GraphOp> {
    public Loss loss;
    public TaskNDManager manager;
    public transient ValueState<Integer> total;
    public transient ValueState<Integer> correct;
    private transient Gauge<Integer> accuracy;

    public Loss createLossFunction() {
        return new SoftmaxCrossEntropyLoss();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        manager = new TaskNDManager();
        loss = createLossFunction();
        ValueStateDescriptor<Integer> totalState = new ValueStateDescriptor<Integer>("total", Integer.class);
        ValueStateDescriptor<Integer> correctState = new ValueStateDescriptor<Integer>("correct", Integer.class);
        total = getRuntimeContext().getState(totalState);
        correct = getRuntimeContext().getState(correctState);
        accuracy = getRuntimeContext().getMetricGroup().gauge("accuracy", () -> {
            try {
                Integer totalValue = total.value();
                if (totalValue == null) totalValue = 0;
                Integer correctValue = correct.value();
                if (correctValue == null) correctValue = 0;
                if (totalValue == 0) return 0;
                return (int) ((double) correctValue / totalValue) * 100;
            } catch (IOException e) {
                return 0;
            }
        });
    }


    public boolean predictedTrue(NDArray logits, NDArray label) throws IOException {
        int maxClassLabel = (int) logits.argMax().getLong();
        int actualLabel = label.getInt();
        return maxClassLabel == actualLabel;

    }

    @Override
    public GraphOp join(GraphOp first, GraphOp second) throws Exception {
        manager.clean();
        // 1. Prepare data
        GradientCollector collector = manager.getTempManager().getEngine().newGradientCollector();
        NDArray logit = ((NDArray) first.element.getFeature("logits").getValue()).expandDims(0);
        logit.setRequiresGradient(true);
        Integer tmp = (Integer) second.element.getFeature("label").getValue();
        NDArray label = manager.getTempManager().create(tmp).expandDims(0);

        // 2. Loss and backward
        NDArray loss = this.loss.evaluate(new NDList(label), new NDList(logit));
        collector.backward(loss);
        // 3. Prepare send data
        logit.getGradient().muli(0.01);
        VTensor grad = new VTensor("grad", new Tuple2<>(logit.getGradient(), 0));
        grad.attachedTo = new Tuple2<>(first.element.elementType(), first.element.getId());
        Rmi backward = new Rmi("trainer", "backward", new Object[]{grad, predictedTrue(logit, label)}, ElementType.PLUGIN, false);

        // 4. Cleanup
        collector.close();
        logit.setRequiresGradient(false);
        return new GraphOp(Op.RMI, first.part_id, backward, IterationType.BACKWARD);
    }
}
