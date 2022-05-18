package functions.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import elements.ElementType;
import elements.Feature;
import elements.GraphElement;
import elements.GraphOp;
import features.Tensor;
import functions.nn.MyParameterStore;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

abstract public class SparseCategoricalCrossEntropyLoss extends ProcessFunction<GraphOp, GraphOp> {
    public final int BATCH_SIZE;
    public Loss lossFn;
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
        Tensor prediction = (Tensor) trainData.element.getFeature("prediction");
        Integer label = ((Feature<Integer, Integer>) trainData.element.getFeature("label")).getValue();
        try {
            if (MyParameterStore.isTensorCorrect(prediction.getValue())) {
                // 1. Initialize some stupid stuff
                NDManager manager = NDManager.newBaseManager();
                GradientCollector collector = manager.getEngine().newGradientCollector();
                // 2. Backward
                prediction.getValue().setRequiresGradient(true);
                NDArray loss = lossFn.evaluate(new NDList(manager.create(label)), new NDList(prediction.getValue()));
                collector.backward(loss);
                System.out.format("Prediction is : %s \n Label is: %s \n", prediction.getValue(), label);
                // 3. Prepare and send data
                GraphElement elementAttached = trainData.element.copy();
                elementAttached.setFeature("grad", new Tensor(prediction.getValue().getGradient().neg().mul(0.01)));
                GraphOp backMsg = new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(elementAttached)
                        .method("backward")
                        .where(MessageDirection.BACKWARD)
                        .addDestination(trainData.partId)
                        .withTimestamp(trainData.getTimestamp())
                        .build().get(0);
                out.collect(backMsg);
                // 4. Cleanup
                manager.close();
                collector.close();
                // Backward Training Start if Batch Size is met
                count++;
                if (count >= BATCH_SIZE) {
                    count = 0;
                    GraphOp trainMsg = new RemoteInvoke()
                            .toElement("trainer", ElementType.PLUGIN)
                            .noUpdate()
                            .withArgs()
                            .method("startTraining")
                            .where(MessageDirection.BACKWARD)
                            .addDestination((short) 0)
                            .withTimestamp(trainData.getTimestamp())
                            .build().get(0);
                    out.collect(trainMsg);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
