package plugins.edge_detection;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.MessageDirection;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            Feature<?, ?> tmp = (Feature<?, ?>) element;
            if (tmp.getName().equals("feature")) {
                Iterable<Edge> edges = storage.getIncidentEdges((Vertex) tmp.getElement(), EdgeType.BOTH);
                if (edges.iterator().hasNext()) {
                    List<Edge> result = new ArrayList<Edge>();
                    edges.forEach(result::add);
                    result.forEach(this::forwardForTraining);
                }

            }
        }
    }

    public void forwardForTraining(Edge e) {
        if (inference.outputReady(e)) {
            NDArray prediction = inference.output((NDArray) e.src.getFeature("feature").getValue(), (NDArray) e.dest.getFeature("feature").getValue(), false);
            Edge messageEdge = e.copy();
            messageEdge.setTimestamp(Math.min(e.src.getFeature("feature").getTimestamp(), e.dest.getFeature("feature").getTimestamp()));
            messageEdge.setFeature("prediction", new VTensor(new Tuple2<>(prediction, inference.MODEL_VERSION)));
            messageEdge.setFeature("label", e.getFeature("label").copy());
            storage.layerFunction.sideMessage(new GraphOp(Op.COMMIT, messageEdge.getPartId(), messageEdge, MessageDirection.FORWARD), trainingOutput);
            e.delete();
        }
    }

    @RemoteFunction
    public void backward(Edge edge) {
        edge.setStorage(this.storage);
        VTensor srcFeature = (VTensor) edge.src.getFeature("feature");
        VTensor destFeature = (VTensor) edge.dest.getFeature("feature");
        VTensor grad = (VTensor) edge.getFeature("grad");
        if (inference.outputReady(edge) && grad.value._2 == inference.MODEL_VERSION && edge.getTimestamp() == Math.min(srcFeature.getTimestamp(), destFeature.getTimestamp())) {
            srcFeature.getValue().setRequiresGradient(true);
            destFeature.getValue().setRequiresGradient(true);
            NDArray prediction = inference.output(srcFeature.getValue(), destFeature.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);
            NDArray srcGrad = srcFeature.getValue().getGradient();
            NDArray destGrad = destFeature.getValue().getGradient();
            if (MyParameterStore.isTensorCorrect(srcGrad)) {
                VTensor srcGradFeature = new VTensor("grad", new Tuple2<>(srcGrad, inference.MODEL_VERSION));
                srcGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                srcGradFeature.setTimestamp(srcFeature.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(srcGradFeature)
                        .addDestination(edge.src.masterPart())
                        .method("backward")
                        .where(MessageDirection.BACKWARD)
                        .buildAndRun(storage);
            }
            if (MyParameterStore.isTensorCorrect(destGrad)) {
                VTensor destGradFeature = new VTensor("grad", new Tuple2<>(destGrad, inference.MODEL_VERSION));
                destGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.dest.getId());
                destGradFeature.setTimestamp(destFeature.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(destGradFeature)
                        .addDestination(edge.dest.masterPart())
                        .method("backward")
                        .where(MessageDirection.BACKWARD)
                        .buildAndRun(storage);
            }
            srcFeature.getValue().setRequiresGradient(false);
            destFeature.getValue().setRequiresGradient(false);
        } else {
            System.out.println("Backward failed since version out-of-date error");
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
        inference = (EdgeOutputInference) this.storage.getPlugin("inferencer");
        trainingOutput = new OutputTag<>("training", TypeInformation.of(GraphOp.class)) {
        };
    }
}
