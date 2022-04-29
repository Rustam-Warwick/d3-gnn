package plugins.embedding_layer;

import aggregators.BaseAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.IterationType;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class GNNEmbeddingLayerTraining extends Plugin {
    public transient GNNEmbeddingLayer inference;
    public transient int collectedGradsSoFar = 0; // Master node collected gradients count

    public GNNEmbeddingLayerTraining() {
        super("trainer");
    }


    @Override
    public void open() {
        super.open();
        inference = (GNNEmbeddingLayer) this.storage.getPlugin("inferencer");
    }

    /**
     * Backward trigger function
     *
     * @param grad grad to be passed for VJP
     */
    @RemoteFunction
    public void backward(VTensor grad) {
        // 1. Get Data
        grad.setStorage(storage);
        Vertex v = (Vertex) grad.getElement();
        VTensor feature = (VTensor) v.getFeature("feature");
        BaseAggregator<?> agg = (BaseAggregator<?>) v.getFeature("agg");
        if (inference.updateReady(v) && grad.value._2 == inference.MODEL_VERSION && grad.getTimestamp() == Math.min(feature.getTimestamp(), agg.getTimestamp())) {
            System.out.println("Processing at position:"+storage.layerFunction.getPosition());
            feature.getValue().setRequiresGradient(true);
            agg.getValue().setRequiresGradient(true);
            // 2. Prediction & Backward
            NDArray prediction = inference.update(feature.getValue(), agg.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);

            // 3. If this is not the last layer, send {dl / dX(l -1)} backwards
            if (!storage.layerFunction.isFirst()) {
                NDArray gradient = feature.getValue().getGradient();
                if (MyParameterStore.isTensorCorrect(gradient)) {
                    VTensor backwardGrad = grad.copy();
                    backwardGrad.value = new Tuple2<>(gradient, inference.MODEL_VERSION);
                    backwardGrad.setTimestamp(feature.getTimestamp());
                    new RemoteInvoke()
                            .toElement("trainer", ElementType.PLUGIN)
                            .noUpdate()
                            .withArgs(backwardGrad)
                            .addDestination(getPartId())
                            .method("backward")
                            .where(IterationType.BACKWARD)
                            .buildAndRun(storage);
                }
            }

            // 4. Send to messageBackward to do the message backward steps
            NDArray gradient = agg.grad();
            if (MyParameterStore.isTensorCorrect(gradient)) {
                grad.value = new Tuple2<>(gradient, inference.MODEL_VERSION);
                grad.setTimestamp(agg.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(grad)
                        .addDestination(getPartId())
                        .addDestinations(agg.replicaParts())
                        .method("messageBackward")
                        .where(IterationType.ITERATE)
                        .buildAndRun(storage);
            }
            // 5. Cleanup
            agg.getValue().setRequiresGradient(false);
            feature.getValue().setRequiresGradient(false);
        }else{
            System.out.println("Failed to backprop");
        }
    }

    /**
     * Backward step for the message function
     *
     * @param aggGrad grad of message output w.r.t loss
     */
    @RemoteFunction
    public void messageBackward(VTensor aggGrad) {
        aggGrad.setStorage(storage);
        if (aggGrad.value._2 == inference.MODEL_VERSION) {
            Vertex vertex = (Vertex) aggGrad.getElement();
            Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
            for (Edge edge : inEdges) {
                if (inference.messageReady(edge) && aggGrad.getTimestamp() > Math.max(edge.getTimestamp(), edge.src.getFeature("feature").getTimestamp())) {
                    // 1. Compute the gradient
                    NDArray inFeature = (NDArray) edge.src.getFeature("feature").getValue();
                    inFeature.setRequiresGradient(true);
                    NDArray prediction = inference.message(inFeature, true);
                    JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
                    NDArray gradient = inFeature.getGradient();
                    // 2. If the gradient is correct and this model is not the first one
                    if (!this.storage.layerFunction.isFirst() && MyParameterStore.isTensorCorrect(gradient)) {
                        VTensor grad = new VTensor("grad", new Tuple2<>(inFeature.getGradient(), inference.MODEL_VERSION));
                        grad.setTimestamp(edge.src.getFeature("feature").getTimestamp());
                        grad.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                        new RemoteInvoke()
                                .toElement("trainer", ElementType.PLUGIN)
                                .noUpdate()
                                .withArgs(grad)
                                .addDestination(edge.src.masterPart())
                                .method("backward")
                                .where(IterationType.BACKWARD)
                                .buildAndRun(storage);
                    }
                    ((NDArray) edge.src.getFeature("feature").getValue()).setRequiresGradient(false);
                }
            }
        }
    }

    /**
     * When Master Receives this message, it starts collecting gradients from replicas
     * Then it performs mean over the batch and updates the model
     */
    @RemoteFunction
    public void startTraining() {
        inference.updatePending = true;
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(IterationType.ITERATE)
                .method("sendGradientsToMaster")
                .addDestinations(replicaParts())
                .withArgs()
                .noUpdate()
                .buildAndRun(storage);

        if (!storage.layerFunction.isFirst()) {
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .where(IterationType.BACKWARD)
                    .method("startTraining")
                    .addDestination(masterPart())
                    .withArgs()
                    .noUpdate()
                    .buildAndRun(storage);
        }
    }

    /**
     * CAll to Sends the local gradients to master
     */
    @RemoteFunction
    public void sendGradientsToMaster() {
        inference.updatePending = true; // Sending to master waiting for new parameters
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(IterationType.ITERATE)
                .method("collectGradients")
                .addDestination(masterPart())
                .withArgs(inference.parameterStore.gradientArrays)
                .noUpdate()
                .buildAndRun(storage);
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
        if (collectedGradsSoFar == replicaParts().size()) {
            collectedGradsSoFar = 0;
            inference.parameterStore.step();
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .where(IterationType.ITERATE)
                    .method("updateParameters")
                    .addDestinations(replicaParts())
                    .addDestination(masterPart())
                    .withArgs(inference.parameterStore.parameterArrays)
                    .noUpdate()
                    .buildAndRun(storage);
        }
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     *
     * @param params
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        inference.parameterStore.updateParameters(params);
        inference.parameterStore.resetGrads();
        inference.MODEL_VERSION++;
        inference.updatePending = false; // Model is here
        // Now we need to do re-inference on all the parts in this instance
        inference.reInferencePending.addAll(storage.layerFunction.getThisParts());
        if (storage.layerFunction.isFirst()) {
            // Re-inference should start from the first layer. Rest is done on the inferencer
            new RemoteInvoke()
                    .toElement(inference.getId(), inference.elementType())
                    .where(IterationType.ITERATE)
                    .method("reInference")
                    .addDestinations(storage.layerFunction.getThisParts())
                    .withArgs()
                    .noUpdate()
                    .buildAndRun(storage);
        }
    }


}
