package functions.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import elements.ElementType;
import elements.Feature;
import elements.GraphOp;
import elements.Op;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.IterationType;
import iterations.Rmi;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

abstract public class EdgeLossFunction extends ProcessFunction<GraphOp, GraphOp> {
    public final int BATCH_SIZE;
    public Loss lossFn;
    transient public int MODEL_VERSION = 0;
    transient public int count = 0;

    public EdgeLossFunction() {
        BATCH_SIZE = 144;
    }

    public EdgeLossFunction(int batch_size) {
        BATCH_SIZE = batch_size;
    }

    public abstract Loss createLossFunction();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lossFn = createLossFunction();
    }

    @Override
    public void processElement(GraphOp trainData, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        VTensor logit = (VTensor) trainData.element.getFeature("prediction");
        Integer label = ((Feature<Integer, Integer>) trainData.element.getFeature("label")).getValue();
        try {
            if (MyParameterStore.isTensorCorrect(logit.getValue()) && logit.value._2 == MODEL_VERSION) {
                NDManager manager = NDManager.newBaseManager();
                GradientCollector collector = manager.getEngine().newGradientCollector();

                NDList labelArray = new NDList(manager.create(label));
                System.out.println(logit.getValue() + ":" + label);
                // 2. Backward
                logit.getValue().setRequiresGradient(true);
                NDArray loss = lossFn.evaluate(labelArray, new NDList(logit.getValue()));
                collector.backward(loss);
                // 3. Prepare and send data
                VTensor grad = (VTensor) logit.copy();
                grad.value = new Tuple2<>(logit.getValue().getGradient().mul(0.001), logit.value._2);
                Rmi backward = new Rmi("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
                out.collect(new GraphOp(Op.RMI, trainData.part_id, backward, IterationType.BACKWARD));

                // 4. Cleanup
                manager.close();
                collector.close();

                // Backward Training Start if Batch Size is met
                count++;
                if (count >= BATCH_SIZE) {
                    count = 0;
                    MODEL_VERSION++;
                    Rmi rmi = new Rmi("trainer", "startTraining", new Object[0], ElementType.PLUGIN, false);
                    out.collect(new GraphOp(Op.RMI, (short) 0, rmi, IterationType.BACKWARD));
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
