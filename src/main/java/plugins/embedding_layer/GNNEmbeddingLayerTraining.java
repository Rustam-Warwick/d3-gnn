package plugins.embedding_layer;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.MessageDirection;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GNNEmbeddingLayerTraining extends Plugin {
    public transient GNNEmbeddingLayer inference;
    public int collectedGradsSoFar = 0; // Master node collected gradients count
    /**
     * Has the master sent the updated parameters, thus ready to start the inference?
     */
    public boolean masterHasSentUpdatedParameters = false;
    /**
     * Master is waiting for the preWatermark
     */
    public boolean masterWaitingForPreWatermark = false;

    public GNNEmbeddingLayerTraining() {
        super("trainer");
    }

    @Override
    public void open() {
        super.open();
        inference = (GNNEmbeddingLayer) this.storage.getPlugin("inferencer");
    }

    /**
     * Backward trigger function.
     * @param grad VTensor that is attached to one vertex. Its value is the gradient of dv(L+1)/dloss
     */
    @RemoteFunction
    public void backward(VTensor grad) {
        // 1. Get Data
        grad.setStorage(storage);
        Vertex v = (Vertex) grad.getElement();
        VTensor feature = (VTensor) v.getFeature("feature");
        BaseAggregator<?> agg = (BaseAggregator<?>) v.getFeature("agg");
        if (inference.updateReady(v) && grad.value._2 == inference.MODEL_VERSION && grad.getTimestamp() == Math.max(feature.getTimestamp(), agg.getTimestamp())) {
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
                            .where(MessageDirection.BACKWARD)
                            .withTimestamp(backwardGrad.getTimestamp())
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
                        .addDestinations(agg.replicaParts())
                        .addDestination(getPartId())
                        .method("messageBackward")
                        .where(MessageDirection.ITERATE)
                        .withTimestamp(grad.getTimestamp())
                        .buildAndRun(storage);
            }
            // 5. Cleanup
            agg.getValue().setRequiresGradient(false);
            feature.getValue().setRequiresGradient(false);
        } else {
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
        Vertex vertex = (Vertex) aggGrad.getElement();
        if (aggGrad.value._2 == inference.MODEL_VERSION) {
            Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
            for (Edge edge : inEdges) {
                if (inference.messageReady(edge) && aggGrad.getTimestamp() >= Math.max(edge.getTimestamp(), edge.src.getFeature("feature").getTimestamp())) {
                    // 1. Compute the gradient
                    VTensor feature = (VTensor) edge.src.getFeature("feature");
                    feature.getValue().setRequiresGradient(true);
                    NDArray prediction = inference.message(feature.getValue(), true);
                    JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
                    NDArray gradient = feature.getValue().getGradient();
                    // 2. If the gradient is correct and this model is not the first one send gradient backwards
                    if (!this.storage.layerFunction.isFirst() && MyParameterStore.isTensorCorrect(gradient)) {
                        VTensor grad = new VTensor("grad", new Tuple2<>(gradient, inference.MODEL_VERSION));
                        grad.setTimestamp(edge.src.getFeature("feature").getTimestamp());
                        grad.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                        new RemoteInvoke()
                                .toElement("trainer", ElementType.PLUGIN)
                                .noUpdate()
                                .withArgs(grad)
                                .addDestination(edge.src.masterPart())
                                .method("backward")
                                .where(MessageDirection.BACKWARD)
                                .withTimestamp(grad.getTimestamp())
                                .buildAndRun(storage);
                    }
                    // 3. Cleanup
                    feature.getValue().setRequiresGradient(false);
                }
            }
        }
    }

    /**
     * When Master Receives this message, it starts collecting gradients from replicas
     * Then it performs mean over the batch and updates the model.
     * Inference model should be frozen at this stage
     * If this is not yet the first layer propogate this message further back across GNN Layers
     */
    @RemoteFunction
    public void startTraining() {
        inference.ACTIVE.replaceAll((key, item)->false);
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(MessageDirection.ITERATE)
                .method("sendGradientsToMaster")
                .addDestinations(othersMasterParts())
                .withArgs()
                .noUpdate()
                .withTimestamp(storage.layerFunction.currentTimestamp())
                .buildAndRun(storage);

        if (!storage.layerFunction.isFirst()) {
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .where(MessageDirection.BACKWARD)
                    .method("startTraining")
                    .addDestination((short) 0)
                    .withTimestamp(storage.layerFunction.currentTimestamp())
                    .withArgs()
                    .noUpdate()
                    .buildAndRun(storage);
        }
    }

    @Override
    public void onPreWatermark(long timestamp) {
        super.onPreWatermark(timestamp);
        if(getPartId() == 0 && masterWaitingForPreWatermark && masterHasSentUpdatedParameters){
            // Ready to call the re-inference methods
            new RemoteInvoke()
                .withTimestamp(storage.layerFunction.currentTimestamp())
                .withArgs()
                .addDestinations(othersMasterParts())
                .addDestination(masterPart())
                .noUpdate()
                .toElement(getId(), ElementType.PLUGIN)
                .method("startReInference")
                .where(MessageDirection.ITERATE)
                .buildAndRun(storage);
            masterWaitingForPreWatermark = false;
            masterHasSentUpdatedParameters = false;
        }
    }


    /**
     * CAll to Sends the local gradients to master
     * This function is called only of masters of operators [1..N] 0(general-master) is exclusive
     */
    @RemoteFunction
    public void sendGradientsToMaster() {
        inference.ACTIVE.replaceAll((key, item)->false);
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(MessageDirection.ITERATE)
                .method("collectGradients")
                .addDestination((short) 0)
                .withArgs(inference.parameterStore.gradientArrays)
                .noUpdate()
                .withTimestamp(storage.layerFunction.currentTimestamp())
                .buildAndRun(storage);
    }

    /**
     * Accumulates all the gradients in master operator
     * @param grads Gradients of model parameters
     */
    @RemoteFunction
    public void collectGradients(Map<String, Tuple2<NDArray, Integer>> grads) {
        inference.parameterStore.meanAccumulateGrads(grads);
        collectedGradsSoFar++;
        if (collectedGradsSoFar == othersMasterParts().size()) {
            collectedGradsSoFar = 0;
            inference.parameterStore.step();
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .where(MessageDirection.ITERATE)
                    .method("updateParameters")
                    .addDestinations(othersMasterParts())
                    .addDestination(masterPart())
                    .withTimestamp(storage.layerFunction.currentTimestamp())
                    .withArgs(inference.parameterStore.parameterArrays)
                    .noUpdate()
                    .buildAndRun(storage);
            if (getPartId() == 0) {
                // Re-inference should start from the first layer. Rest is done on the inferencer
                masterHasSentUpdatedParameters = true;
                if(storage.layerFunction.isFirst()) masterWaitingForPreWatermarkOn();
            }
        }
    }

    @RemoteFunction
    public void masterWaitingForPreWatermarkOn(){
        masterWaitingForPreWatermark = true;
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     * @param params Parameters for this model
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        inference.parameterStore.updateParameters(params);
        inference.parameterStore.resetGrads();
        inference.MODEL_VERSION ++;
    }

    @RemoteFunction
    public void startReInference(){
        new RemoteInvoke()
                .where(MessageDirection.ITERATE)
                .noUpdate()
                .withArgs()
                .method("reInference")
                .addDestinations(replicaParts())
                .addDestination(masterPart())
                .withTimestamp(storage.layerFunction.currentTimestamp())
                .toElement(getId(), ElementType.PLUGIN)
                .buildAndRun(storage);
    }

    /**
     * New Parameters have been committed, need to increment the model version
     */
    @RemoteFunction
    public void reInference() {
        for (Vertex vertex : storage.getVertices()) {
            Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
            List<NDArray> bulkReduceMessages = new ArrayList<>();
            long maxTs = Long.MIN_VALUE;
            for (Edge edge : inEdges) {
                if (inference.messageReady(edge)) {
                    NDArray msg = inference.message(((VTensor) edge.src.getFeature("feature")).getValue(), false);
                    bulkReduceMessages.add(msg);
                    maxTs = Math.max(maxTs, Math.max(edge.getTimestamp(), edge.src.getFeature("feature").getTimestamp()));
                }
            }
            if (bulkReduceMessages.size() > 0) {
                NDArray msgs = MeanAggregator.bulkReduce(bulkReduceMessages.toArray(NDArray[]::new));
                new RemoteInvoke()
                        .toElement(vertex.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(vertex.masterPart())
                        .withTimestamp(maxTs)
                        .withArgs(inference.MODEL_VERSION, msgs, bulkReduceMessages.size())
                        .buildAndRun(storage);
            }
        }
        inference.ACTIVE.put(getPartId(), true);
    }
}
