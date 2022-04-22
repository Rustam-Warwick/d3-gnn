package plugins;

import aggregators.BaseAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.IterationType;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rmi;
import scala.Tuple2;

import java.util.Map;

public class GNNLayerTraining extends Plugin {
    public transient GNNLayerInference inference;
    public transient int collectedGradsSoFar = 0; // Master node collected gradients count

    public GNNLayerTraining() {
        super("trainer");
    }


    @Override
    public void open() {
        super.open();
        inference = (GNNLayerInference) this.storage.getPlugin("inferencer");
    }

    /**
     * Backward trigger function
     * @param grad grad to be passed for VJP
     */
    @RemoteFunction
    public void backward(VTensor grad) {
        // 1. Get Data
        grad.setStorage(this.storage);
        Vertex v = (Vertex) grad.getElement();
        if (inference.updateReady(v) && grad.value._2 == inference.MODEL_VERSION) {
            VTensor feature = (VTensor) grad.getElement().getFeature("feature");
            BaseAggregator<?> agg = (BaseAggregator<?>) grad.getElement().getFeature("agg");
            feature.getValue().setRequiresGradient(true);
            agg.getValue().setRequiresGradient(true);

            // 2. Prediction & Backward
            NDArray prediction = this.inference.update(feature.getValue(), agg.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);

            // 3. Send Update backward if this is not last layer
            if (!this.storage.layerFunction.isFirst()) {
                NDArray gradient = feature.getValue().getGradient();
                if (MyParameterStore.isTensorCorrect(gradient)) {
                    grad.value = new Tuple2<>(gradient, inference.MODEL_VERSION);
                    Rmi backward = new Rmi("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
                    this.storage.layerFunction.message(new GraphOp(Op.RMI, this.storage.layerFunction.getCurrentPart(), backward, IterationType.BACKWARD));
                }
            }

            // 4. Send to messageBackward to do the message backward steps
            NDArray gradient = agg.grad();
            if (MyParameterStore.isTensorCorrect(gradient)) {
                grad.value = new Tuple2<>(gradient, inference.MODEL_VERSION);
                Rmi.callProcedure(this, "messageBackward", IterationType.ITERATE, agg.replicaParts(), grad);
                this.messageBackward(grad);
            }
            // 5. Cleanup
            agg.getValue().setRequiresGradient(false);
            feature.getValue().setRequiresGradient(false);
        }
    }

    /**
     * Backward step for the message function
     *
     * @param aggGrad grad of message output w.r.t loss
     */
    @RemoteFunction
    public void messageBackward(VTensor aggGrad) {
        aggGrad.setStorage(this.storage);
        if (aggGrad.value._2 == inference.MODEL_VERSION) {
            Vertex vertex = (Vertex) aggGrad.getElement();
            Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
            for (Edge edge : inEdges) {
                if (this.inference.messageReady(edge)) {
                    NDArray inFeature = (NDArray) edge.src.getFeature("feature").getValue();
                    inFeature.setRequiresGradient(true);
                    NDArray prediction = this.inference.message(inFeature, true);
                    JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
                    NDArray gradient = inFeature.getGradient();
                    if (!this.storage.layerFunction.isFirst() && MyParameterStore.isTensorCorrect(gradient)) {
                        VTensor grad = new VTensor("grad", new Tuple2<>(inFeature.getGradient(), inference.MODEL_VERSION));
                        grad.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                        Rmi backward = new Rmi("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
                        this.storage.layerFunction.message(new GraphOp(Op.RMI, edge.src.masterPart(), backward, IterationType.BACKWARD));
                    }
                    ((NDArray) edge.src.getFeature("feature").getValue()).setRequiresGradient(false);
                }
            }
        }
    }

    @RemoteFunction
    public void startTraining() {
        Rmi.callProcedure(this, "callForGradientCollection", IterationType.ITERATE, RemoteDestination.ALL);
        if (!storage.layerFunction.isFirst()) {
            Rmi.callProcedure(this, "startTraining", IterationType.BACKWARD, RemoteDestination.MASTER);
        }
    }

    @RemoteFunction
    public void callForGradientCollection() {
        inference.updatePending = true;
        Rmi.callProcedure(this, "collectGradients", inference.parameterStore.gradientArrays);
    }


    /**
     * Accumulates all the gradients in master operator
     *
     * @param grads
     */
    @RemoteFunction
    public void collectGradients(Map<String, Tuple2<NDArray, Integer>> grads) {
        inference.parameterStore.meanAccumulateGrads(grads);
        collectedGradsSoFar++;
        if (collectedGradsSoFar == replicaParts().size() + 1) {
            collectedGradsSoFar = 0;
            inference.parameterStore.step();
            Rmi.callProcedure(this, "updateParameters", IterationType.ITERATE, RemoteDestination.ALL, this.inference.parameterStore.parameterArrays);
        }
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     *
     * @param params
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        this.inference.parameterStore.updateParameters(params);
        this.inference.parameterStore.resetGrads();
        this.inference.MODEL_VERSION++;
        inference.updatePending = false;
        if(storage.layerFunction.isFirst()){
            // Re-inference should start from the first layer. Rest is done on the inferencer
            Rmi.callProcedure(inference, "reInference", IterationType.ITERATE, this.storage.layerFunction.getThisParts());
        }
    }



}
