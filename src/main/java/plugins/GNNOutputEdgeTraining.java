package plugins;

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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class GNNOutputEdgeTraining extends Plugin {
    public transient GNNOutputEdgeInference inference;
    public transient OutputTag<GraphOp> trainingOutput;
    public transient int collectedGradsSoFar = 0;

    public GNNOutputEdgeTraining() {
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
            Feature tmp = (Feature) element;
            if (tmp.getFieldName().equals("feature")) {
                storage.getIncidentEdges((Vertex) tmp.getElement(), EdgeType.BOTH).forEach(this::forwardForTraining);
            }
        }
    }

    public void forwardForTraining(Edge e) {
        if (inference.outputReady(e)) {
            NDArray prediction = inference.output((NDArray) e.src.getFeature("feature").getValue(), (NDArray) e.dest.getFeature("feature").getValue(), false);
            Edge messageEdge = (Edge) e.copy();
            messageEdge.setFeature("prediction", new VTensor(new Tuple2<>(prediction, inference.MODEL_VERSION)));
            messageEdge.setFeature("label", e.getFeature("label"));
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

    @RemoteFunction
    public void startTraining() {
        Rmi.callProcedure(this, "callForGradientCollection", IterationType.ITERATE, RemoteDestination.ALL);
        Rmi.callProcedure(this, "startTraining", IterationType.BACKWARD, RemoteDestination.MASTER);
    }

    @RemoteFunction
    public void callForGradientCollection() {
        inference.updatePending = true;
        Rmi.callProcedure(this, "collectGradients", inference.parameterStore.gradientArrays);
    }


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

    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        inference.parameterStore.updateParameters(params);
        inference.parameterStore.resetGrads();
        inference.MODEL_VERSION++;
        inference.updatePending = false;
    }


    @Override
    public void open() {
        super.open();
        inference = (GNNOutputEdgeInference) this.storage.getPlugin("inferencer");
        trainingOutput = new OutputTag<>("training", TypeInformation.of(GraphOp.class)) {
        };
    }
}
