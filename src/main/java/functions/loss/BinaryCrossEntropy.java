package functions.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.training.loss.Loss;

public class BinaryCrossEntropy extends Loss {
    public BinaryCrossEntropy(String name) {
        super(name);
    }

    public BinaryCrossEntropy() {
        super("BinaryCrossEntropy");
    }

    @Override
    public NDArray evaluate(NDList labels, NDList predictions) {
        NDArray lab = labels.singletonOrThrow(); // logits
        NDArray pred = predictions.singletonOrThrow(); // prediction
        NDArray one = pred.getManager().create(1);
        NDArray inverseLabel = one.sub(lab);
        NDArray inversePred = one.sub(pred);
        NDArray logPred = pred.log();
        NDArray inverseLogPred = inversePred.log();
        return lab.mul(logPred).add(
                inverseLabel.mul(inverseLogPred)
        ).neg();
    }
}
