package functions.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.training.loss.Loss;

public class SparseCategoricalCrossEntropy extends Loss {
    public SparseCategoricalCrossEntropy(String name) {
        super(name);
    }

    public SparseCategoricalCrossEntropy() {
        super("BinaryCrossEntropy");
    }

    @Override
    public NDArray evaluate(NDList labels, NDList predictions) {
        NDArray label = labels.singletonOrThrow();
        NDArray pred = predictions.singletonOrThrow();
        NDArray predLog = pred.log();
        return label.dot(predLog).neg();
    }
}
