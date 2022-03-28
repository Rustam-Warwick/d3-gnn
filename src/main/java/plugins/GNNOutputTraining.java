package plugins;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.watermark.Watermark;
import scala.Tuple2;

import java.util.Map;

public class GNNOutputTraining extends Plugin {
    public GNNOutputInference inference;
    public boolean waitingForUpdate = false;
    public int total = 1;
    public int totalCorrect = 1;
    public int collectedGradsSoFar = 0;
    public GNNOutputTraining(){
        super("trainer");
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if(newElement.elementType() == ElementType.VERTEX){
            if(newElement.getFeature("label") == null){
                // Label exists

            }
        }
    }

    @RemoteFunction
    public void backward(VTensor grad, boolean correctlyPredicted){
        // 0. Update Gauge
        total++;
        if(correctlyPredicted) totalCorrect++;
        // 1. Get Data
        grad.setStorage(this.storage);
        VTensor feature = (VTensor) grad.getElement().getFeature("feature");
        feature.getValue().setRequiresGradient(true);
        // 2. Backward
        NDArray prediction = this.inference.output(feature.getValue(), true);
//        NDArray prediction = this.inference.outputModel.getBlock().forward(this.inference.parameterStore, new NDList(feature.getValue()),true).get(0);
        JniUtils.backward((PtNDArray) prediction, (PtNDArray)grad.getValue() ,false, false);

        // 3. Send Data back
        grad.value = new Tuple2<>(feature.getValue().getGradient(), 0);
        Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
        this.storage.message(new GraphOp(Op.RPC, this.storage.currentKey, backward, IterationState.BACKWARD));

        // 4. Cleanup
        feature.getValue().setRequiresGradient(false);
    }

    @RemoteFunction
    public void collectGradients(Map<String, NDArray> grads){
        this.inference.parameterStore.addGrads(grads);
        collectedGradsSoFar++;
        if(collectedGradsSoFar == this.storage.parallelism){
            this.inference.parameterStore.updateAllParameters();
            Rpc.callProcedure(this, "updateParameters", IterationState.ITERATE, RemoteDestination.REPLICAS, this.inference.parameterStore.parameterArrays);
            collectedGradsSoFar = 0;
        }
    }

    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params){
        this.inference.parameterStore.updateParameters(params);
        this.inference.parameterStore.resetGrads();
        waitingForUpdate = false;
        this.inference.MODEL_VERSION++;
    }


    @Override
    public void open() {
        super.open();
        inference = (GNNOutputInference) this.storage.getPlugin("inferencer");
        storage.getRuntimeContext().getMetricGroup().gauge("accuracy", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return (int)((double) totalCorrect / total * 1000);
            }
        });
    }
}
