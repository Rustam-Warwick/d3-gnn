package plugins.edge_detection;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class EdgeOutputTraining extends Plugin {
    public transient EdgeOutputInference inference;
    public transient OutputTag<GraphOp> trainingOutput;
    public transient int collectedGradsSoFar = 0;

    public EdgeOutputTraining() {
        super("trainer");
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            forwardForTraining(edge);
        }
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?,?> tmp = (Feature<?,?>) element;
            if (tmp.getName().equals("feature")) {
                storage.getIncidentEdges((Vertex) tmp.getElement(), EdgeType.BOTH).forEach(this::forwardForTraining);
            }
        }
    }

    public void forwardForTraining(Edge e) {
        if (inference.outputReady(e)) {
            NDArray prediction = inference.output((NDArray) e.src.getFeature("feature").getValue(), (NDArray) e.dest.getFeature("feature").getValue(), false);
            Edge messageEdge =  e.copy();
            messageEdge.setFeature("prediction", new VTensor(new Tuple2<>(prediction, inference.MODEL_VERSION)));
            messageEdge.setFeature("label", e.getFeature("label").copy());
            storage.layerFunction.sideMessage(new GraphOp(Op.COMMIT, messageEdge.getPartId(), messageEdge, IterationType.FORWARD), trainingOutput);
        }
    }

    @RemoteFunction
    public void backward(VTensor grad) {
        grad.setStorage(this.storage);
        Edge edge = (Edge) grad.getElement();
        if (Objects.nonNull(edge) && inference.outputReady(edge) && grad.value._2 == inference.MODEL_VERSION) {
            VTensor srcFeature = (VTensor) edge.src.getFeature("feature");
            VTensor destFeature = (VTensor) edge.dest.getFeature("feature");
            srcFeature.getValue().setRequiresGradient(true);
            destFeature.getValue().setRequiresGradient(true);
            NDArray prediction = inference.output(srcFeature.getValue(), destFeature.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);
            NDArray srcGrad = srcFeature.getValue().getGradient();
            NDArray destGrad = destFeature.getValue().getGradient();
            if (MyParameterStore.isTensorCorrect(srcGrad)) {
                VTensor srcGradFeature = new VTensor("grad", new Tuple2<>(srcGrad, inference.MODEL_VERSION));
                srcGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                Rmi backwardSrc = new Rmi("trainer", "backward", new Object[]{srcGradFeature}, ElementType.PLUGIN, false);
                storage.layerFunction.message(new GraphOp(Op.RMI, edge.src.masterPart(), backwardSrc, IterationType.BACKWARD));
            }
            if (MyParameterStore.isTensorCorrect(destGrad)) {
                VTensor destGradFeature = new VTensor("grad", new Tuple2<>(destGrad, inference.MODEL_VERSION));
                destGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.dest.getId());
                Rmi backwardDest = new Rmi("trainer", "backward", new Object[]{destGradFeature}, ElementType.PLUGIN, false);
                storage.layerFunction.message(new GraphOp(Op.RMI, edge.dest.masterPart(), backwardDest, IterationType.BACKWARD));
            }
            srcFeature.getValue().setRequiresGradient(false);
            destFeature.getValue().setRequiresGradient(false);
//            storage.layerFunction.message(new GraphOp(Op.COMMIT, getPartId(), edge, IterationType.BACKWARD));
            edge.deleteElement(); // Delete so that this edge is not trained anymore
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
        inference = (EdgeOutputInference) this.storage.getPlugin("inferencer");
        trainingOutput = new OutputTag<>("training", TypeInformation.of(GraphOp.class)) {
        };
    }
}
