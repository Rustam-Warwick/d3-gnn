package plugins;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.ElementType;
import elements.GraphOp;
import elements.Op;
import elements.Plugin;
import features.VTensor;
import helpers.JavaTensor;
import iterations.IterationState;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple2;

public class GNNOutputTraining extends Plugin {
    public GNNOutputInference inference = null;
    public GNNOutputTraining(){
        super("trainer");
    }

    @RemoteFunction
    public void backward(VTensor grad){
        // 1. Get Data
        grad.setStorage(this.storage);
        VTensor feature = (VTensor) grad.getElement().getFeature("feature");
        feature.getValue().setRequiresGradient(true);

        // 2. Backward
        NDArray prediction = this.inference.outputModel.getBlock().forward(this.inference.parameterStore, new NDList(feature.getValue()), true).get(0);
        JniUtils.backward((PtNDArray) prediction, null, false, false);

        // 3. Send Data back
        grad.value = new Tuple2<>(JavaTensor.of(feature.getValue().getGradient()), 0);
        Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
        this.storage.message(new GraphOp(Op.RPC, this.storage.currentKey, backward, IterationState.BACKWARD));

        // 4. Cleanup
        feature.getValue().setRequiresGradient(false);
    }

    @Override
    public void open() {
        super.open();
        inference = (GNNOutputInference) this.storage.getPlugin("inferencer");
    }
}
