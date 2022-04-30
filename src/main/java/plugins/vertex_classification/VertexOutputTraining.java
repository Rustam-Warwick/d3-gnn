package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.MessageDirection;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import iterations.Rmi;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class VertexOutputTraining extends Plugin {
    public VertexOutputInference inference;
    public transient OutputTag<GraphOp> trainingOutput;
    public transient int collectedGradsSoFar = 0;

    public VertexOutputTraining() {
        super("trainer");
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
            if (newFeature.getName().equals("feature") && Objects.nonNull(newFeature.getElement().getFeature("label"))) {
                // This is a new feature update with an existing label so training can be done if the ouput is ready as well
                forwardForTraining((Vertex) newFeature.getElement());
            }
        }
    }

    public void forwardForTraining(Vertex v) {
        if (inference.outputReady(v)) {
            NDArray output = inference.output((NDArray) v.getFeature("feature").getValue(), false);
            Vertex vcopy = v.copy();
            vcopy.setFeature("prediction", new VTensor(new Tuple2<>(output, inference.MODEL_VERSION)));
            vcopy.setFeature("label", v.getFeature("label").copy());
            storage.layerFunction.sideMessage(new GraphOp(Op.COMMIT, vcopy.getPartId(), vcopy, MessageDirection.FORWARD), trainingOutput);
        }
    }

    @RemoteFunction
    public void backward(VTensor grad) {
        grad.setStorage(this.storage);
        Vertex vertex = (Vertex) grad.getElement();
        if (Objects.nonNull(vertex) && inference.outputReady(vertex) && grad.value._2 == inference.MODEL_VERSION) {
            VTensor feature = (VTensor) vertex.getFeature("feature");
            feature.getValue().setRequiresGradient(true);
            NDArray prediction = inference.output(feature.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);
            NDArray vertexGrad = feature.getValue().getGradient();
            if (MyParameterStore.isTensorCorrect(vertexGrad)) {
                VTensor srcGradFeature = new VTensor("grad", new Tuple2<>(vertexGrad, inference.MODEL_VERSION));
                srcGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, vertex.getId());
                Rmi backwardSrc = new Rmi("trainer", "backward", new Object[]{srcGradFeature}, ElementType.PLUGIN, false);
                storage.layerFunction.message(new GraphOp(Op.RMI, vertex.masterPart(), backwardSrc, MessageDirection.BACKWARD));
            }
            feature.getValue().setRequiresGradient(false);
            if (vertex.getFeature("label") != null) vertex.getFeature("label").delete();
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
                .where(MessageDirection.ITERATE)
                .method("sendGradientsToMaster")
                .addDestinations(replicaParts())
                .withArgs()
                .noUpdate()
                .buildAndRun(storage);

        if (!storage.layerFunction.isFirst()) {
            new RemoteInvoke()
                    .toElement(getId(), elementType())
                    .where(MessageDirection.BACKWARD)
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
                .where(MessageDirection.ITERATE)
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
                    .where(MessageDirection.ITERATE)
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
    }

    @Override
    public void open() {
        super.open();
        inference = (VertexOutputInference) this.storage.getPlugin("inferencer");
        trainingOutput = new OutputTag<>("training", TypeInformation.of(GraphOp.class)) {
        };
    }
}
