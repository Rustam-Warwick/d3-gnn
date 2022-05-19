package plugins.embedding_layer;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import functions.nn.MyParameterStore;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GNNEmbeddingLayerTraining extends Plugin {
    public transient GNNEmbeddingLayer inference;
    public Long reInferenceWatermark = null; // Next watermark of iteration 1 should be re-Inference
    public Long sendAlignWatermark = null; // Should I send Align watermarks to the next layer on watermark
    public boolean alignWatermarks = false; // Should the master(0) align the watermarks of replicas ?
    public int collectedGradsSoFar = 0; // Master node collected gradients count

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
     *
     * @param grad Tensor that is attached to one vertex. Its value is the gradient of dv(L+1)/dloss
     */
    @RemoteFunction
    public void backward(Tensor grad) {
        // 1. Get Data
        grad.setStorage(storage);
        Vertex v = (Vertex) grad.getElement();
        Tensor feature = (Tensor) v.getFeature("feature");
        BaseAggregator<?> agg = (BaseAggregator<?>) v.getFeature("agg");
        if (inference.ACTIVE && inference.updateReady(v) && grad.getTimestamp() == Math.max(feature.getTimestamp(), agg.getTimestamp())) {
            feature.getValue().setRequiresGradient(true);
            agg.getValue().setRequiresGradient(true);
            // 2. Prediction & Backward
            NDArray prediction = inference.update(feature.getValue(), agg.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);

            // 3. If this is not the last layer, send {dl / dX(l -1)} backwards
            if (!storage.layerFunction.isFirst()) {
                NDArray gradient = feature.getValue().getGradient();
                if (MyParameterStore.isTensorCorrect(gradient)) {
                    Tensor backwardGrad = grad.copy();
                    backwardGrad.value = gradient;
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
                Tensor aggGrad = grad.copy();
                aggGrad.value = gradient;
                aggGrad.setTimestamp(agg.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(aggGrad)
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
    public void messageBackward(Tensor aggGrad) {
        if (inference.ACTIVE) {
            aggGrad.setStorage(storage);
            Vertex vertex = (Vertex) aggGrad.getElement();
            Iterable<Edge> inEdges = storage.getIncidentEdges(vertex, EdgeType.IN);
            for (Edge edge : inEdges) {
                if (inference.messageReady(edge) && aggGrad.getTimestamp() >= Math.max(edge.getTimestamp(), edge.src.getFeature("feature").getTimestamp())) {
                    // 1. Compute the gradient
                    Tensor feature = (Tensor) edge.src.getFeature("feature");
                    feature.getValue().setRequiresGradient(true);
                    NDArray prediction = inference.message(feature.getValue(), true);
                    JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
                    NDArray gradient = feature.getValue().getGradient();
                    // 2. If the gradient is correct and this model is not the first one send gradient backwards
                    if (!storage.layerFunction.isFirst() && MyParameterStore.isTensorCorrect(gradient)) {
                        Tensor grad = new Tensor("grad", gradient);
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

    // ---- PARAMETER UPDATES START

    /**
     * When Master Receives this message, it starts collecting gradients from replicas
     * Then it performs mean over the batch and updates the model.
     * Inference model should be frozen at this stage
     * If this is not yet the first layer propogate this message further back across GNN Layers
     */
    @RemoteFunction
    public void startTraining() {
        assert getPartId() == 0;
        inference.ACTIVE = false;
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(MessageDirection.ITERATE)
                .method("sendGradientsToMaster")
                .addDestinations(othersMasterParts())
                .withArgs()
                .noUpdate()
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

    /**
     * CAll to Sends the local gradients to master
     * This function is called only of masters of operators [1..N] 0(general-master) is exclusive
     */
    @RemoteFunction
    public void sendGradientsToMaster() {
        inference.ACTIVE = false;
        new RemoteInvoke()
                .toElement(getId(), elementType())
                .where(MessageDirection.ITERATE)
                .method("collectGradients")
                .addDestination((short) 0)
                .withArgs(inference.parameterStore.gradientArrays)
                .noUpdate()
                .buildAndRun(storage);
    }

    /**
     * Accumulates all the gradients in master operator
     *
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
                    .withArgs(inference.parameterStore.parameterArrays)
                    .noUpdate()
                    .buildAndRun(storage);
            if (storage.layerFunction.isFirst()) alignWatermarks = true;
        }
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     *
     * @param params Parameters for this model
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        inference.parameterStore.updateParameters(params);
        inference.parameterStore.resetGrads();
    }

    // ---- PARAMETER UPDATES END

    // ---- REINFERENCE START
    @Override
    public void onWatermark(long timestamp) {
        super.onWatermark(timestamp);
        if (alignWatermarks && getPartId() == 0 && timestamp % 4 == 0) {
            // Updates sent from master but trigger not yet started
            new RemoteInvoke()
                    .toElement(getId(), ElementType.PLUGIN)
                    .where(MessageDirection.ITERATE)
                    .method("setReInferenceWatermark")
                    .addDestinations(othersMasterParts())
                    .addDestination(getPartId())
                    .withArgs(timestamp + 1)
                    .noUpdate()
                    .buildAndRun(storage);

            if (!storage.layerFunction.isLast()) sendAlignWatermark = timestamp + 3;
            alignWatermarks = false;
        }
        if (reInferenceWatermark != null && timestamp >= reInferenceWatermark) {
            reInference(timestamp);
            if (getPartId() == replicaParts().get(replicaParts().size() - 1)) {
                // Last element so need to clear the
                System.out.format("Re Inference at subtask %s, position %s \n", storage.layerFunction.getRuntimeContext().getIndexOfThisSubtask(), storage.layerFunction.getPosition());
                reInferenceWatermark = null;
                inference.ACTIVE = true;
            }
        }
        if (sendAlignWatermark != null && timestamp >= sendAlignWatermark && getPartId() == 0) {
            new RemoteInvoke()
                    .addDestination((short) 0)
                    .toElement(getId(), ElementType.PLUGIN)
                    .noUpdate()
                    .method("setAlignWatermarks")
                    .withArgs()
                    .where(MessageDirection.FORWARD)
                    .buildAndRun(storage);
            sendAlignWatermark = null;
        }
    }

    @RemoteFunction
    public void setAlignWatermarks() {
        alignWatermarks = true;
    }

    @RemoteFunction
    public void setReInferenceWatermark(long ts) {
        reInferenceWatermark = ts;
    }


    public void reInference(long timestamp) {
        for (Vertex vertex : storage.getVertices()) {
            if (Objects.nonNull(vertex.getFeature("agg"))) {
                // @todo Fix this can be out of sync with the updates
                ((BaseAggregator<?>) vertex.getFeature("agg")).reset();
            }
            Iterable<Edge> inEdges = storage.getIncidentEdges(vertex, EdgeType.IN);
            List<NDArray> bulkReduceMessages = new ArrayList<>();
            long maxTs = timestamp;
            for (Edge edge : inEdges) {
                if (inference.messageReady(edge)) {
                    NDArray msg = inference.message((NDArray) (edge.src.getFeature("feature")).getValue(), false);
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
                        .withArgs(msgs, bulkReduceMessages.size())
                        .buildAndRun(storage);
            } else if (Objects.nonNull(vertex.getFeature("agg"))) {
                // If there are no in-messages for aggregator, its timestamp might not be updated.
                // Update it here so that it is inferred onWatermark. Update can be local since MASTER part is using it
                vertex.getFeature("agg").resolveTimestamp(timestamp);
                storage.updateFeature(vertex.getFeature("agg"));
            }
        }
    }
}
