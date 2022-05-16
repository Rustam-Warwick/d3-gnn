package ai.djl.training.loss;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.index.NDIndex;

public class CrossEntropyLoss extends Loss {
    public boolean sparseLabel;

    public CrossEntropyLoss(String name) {
        this(name, true);
    }

    public CrossEntropyLoss(String name, boolean sparseLabel) {
        super(name);
        this.sparseLabel = sparseLabel;
    }

    @Override
    public NDArray evaluate(NDList label, NDList prediction) {
        NDArray pred = prediction.singletonOrThrow();
        NDArray loss;
        NDArray lab = label.singletonOrThrow();
        if (sparseLabel) {
            NDIndex pickIndex =
                    new NDIndex()
                            .addAllDim(Math.floorMod(-1, pred.getShape().dimension()))
                            .addPickDim(lab);
            loss = pred.get(pickIndex).log2();
        } else {
            lab = lab.reshape(pred.getShape());
            loss = pred.mul(lab).neg().sum(new int[]{-1}, true);
        }
//        if (weight != 1) {
//            loss = loss.mul(weight);
//        }

        return loss.mean().neg();
    }
}
