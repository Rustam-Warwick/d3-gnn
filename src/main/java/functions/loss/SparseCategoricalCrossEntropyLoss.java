package functions.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import elements.Feature;
import elements.GraphElement;
import elements.GraphOp;
import features.VTensor;
import helpers.MyParameterStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

abstract public class SparseCategoricalCrossEntropyLoss extends ProcessFunction<GraphOp, GraphOp> {
    public final int BATCH_SIZE;
    public Loss lossFn;
    transient public int MODEL_VERSION = 0;
    transient public int count = 0;

    public SparseCategoricalCrossEntropyLoss() {
        BATCH_SIZE = 144;
    }

    public SparseCategoricalCrossEntropyLoss(int batch_size) {
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
        VTensor prediction = (VTensor) trainData.element.getFeature("prediction");
        Integer label = ((Feature<Integer, Integer>) trainData.element.getFeature("label")).getValue();
        try {
            if (MyParameterStore.isTensorCorrect(prediction.getValue()) && prediction.value._2 == MODEL_VERSION) {
                // 1. Initialize some stupid stuff
                NDManager manager = NDManager.newBaseManager();
                GradientCollector collector = manager.getEngine().newGradientCollector();
                // 2. Backward
                prediction.getValue().setRequiresGradient(true);
                NDArray loss = lossFn.evaluate(new NDList(manager.create(label)), new NDList(prediction.getValue()));
                collector.backward(loss);
                System.out.println(prediction.getValue());
                // 3. Prepare and send data
                GraphElement elementAttached = trainData.element.copy();
                elementAttached.setFeature("grad", new VTensor(new Tuple2<>(prediction.getValue().getGradient().neg().mul(0.01), prediction.value._2)));
//                Rmi backward = new Rmi("trainer", "backward", new Object[]{elementAttached}, ElementType.PLUGIN, false);
//                out.collect(new GraphOp(Op.RMI, trainData.part_id, backward, MessageDirection.BACKWARD));
                // 4. Cleanup
                manager.close();
                collector.close();
                // Backward Training Start if Batch Size is met
                count++;
                if (count >= BATCH_SIZE) {
                    count = 0;
                    MODEL_VERSION++;
//                    Rmi rmi = new Rmi("trainer", "startTraining", new Object[0], ElementType.PLUGIN, false);
//                    out.collect(new GraphOp(Op.RMI, (short) 0, rmi, MessageDirection.BACKWARD));
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
