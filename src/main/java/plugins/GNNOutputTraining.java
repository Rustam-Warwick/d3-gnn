package plugins;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.training.GradientCollector;
import ai.djl.pytorch.jni.JniUtils;
import elements.Plugin;
import features.Tensor;
import features.VTensor;
import iterations.RemoteFunction;

public class GNNOutputTraining extends Plugin {
    public GNNOutputInference inference = null;
    public GNNOutputTraining(){
        super("trainer");
    }

    @RemoteFunction
    public void backward(Tensor grad){
        grad.setStorage(this.storage);
        VTensor feature = (VTensor) grad.getElement().getFeature("feature");
        GradientCollector collector = this.storage.tensorManager.getEngine().newGradientCollector();
        feature.getValue().setRequiresGradient(true);

        NDArray prediction = this.inference.outputModel.getBlock().forward(this.inference.parameterStore, new NDList(feature.getValue()), false).get(0);
        JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);



        collector.close();
        feature.getValue().setRequiresGradient(false);
    }

    @Override
    public void open() {
        super.open();
        inference = (GNNOutputInference) this.storage.getPlugin("inferencer");
    }
}
