package plugins.edge_detection;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import features.Tensor;
import functions.nn.MyParameterStore;
import iterations.MessageDirection;
import iterations.RemoteFunction;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EdgeOutputTraining extends Plugin {
    public transient EdgeOutputInference inference;
    public transient int collectedGradsSoFar = 0;
    public boolean removeFeaturesOnNextWatermark = false;
    public EdgeOutputTraining() {
        super("trainer");
    }

    @Override
    public void open() {
        super.open();
        inference = (EdgeOutputInference) this.storage.getPlugin("inferencer");
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
        if (inference.ACTIVE && inference.outputReady(e) && storage.layerFunction.getTimerService().currentWatermark() >= Math.max(e.src.getFeature("feature").getTimestamp(), e.dest.getFeature("feature").getTimestamp())) {
            NDArray prediction = inference.output((NDArray) e.src.getFeature("feature").getValue(), (NDArray) e.dest.getFeature("feature").getValue(), false);
            Edge messageEdge = e.copy();
            messageEdge.setTimestamp(Math.max(e.src.getFeature("feature").getTimestamp(), e.dest.getFeature("feature").getTimestamp()));
            messageEdge.setFeature("prediction", new Tensor(prediction));
            messageEdge.setFeature("label", e.getFeature("label").copy());
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageEdge.getPartId(), messageEdge, MessageDirection.FORWARD, messageEdge.getTimestamp()));
            e.delete();
        }
    }

    @RemoteFunction
    public void backward(Edge edge) {
        edge.setStorage(this.storage);
        Tensor srcFeature = (Tensor) edge.src.getFeature("feature");
        Tensor destFeature = (Tensor) edge.dest.getFeature("feature");
        Tensor grad = (Tensor) edge.getFeature("grad");
        if (inference.ACTIVE && inference.outputReady(edge) && edge.getTimestamp() == Math.max(srcFeature.getTimestamp(), destFeature.getTimestamp())) {
            srcFeature.getValue().setRequiresGradient(true);
            destFeature.getValue().setRequiresGradient(true);
            NDArray prediction = inference.output(srcFeature.getValue(), destFeature.getValue(), true);
            JniUtils.backward((PtNDArray) prediction, (PtNDArray) grad.getValue(), false, false);
            NDArray srcGrad = srcFeature.getValue().getGradient();
            NDArray destGrad = destFeature.getValue().getGradient();
            if (MyParameterStore.isTensorCorrect(srcGrad)) {
                Tensor srcGradFeature = new Tensor("grad", srcGrad);
                srcGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.src.getId());
                srcGradFeature.setTimestamp(srcFeature.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(srcGradFeature)
                        .withTimestamp(srcGradFeature.getTimestamp())
                        .addDestination(edge.src.masterPart())
                        .method("backward")
                        .where(MessageDirection.BACKWARD)
                        .buildAndRun(storage);
            }
            if (MyParameterStore.isTensorCorrect(destGrad)) {
                Tensor destGradFeature = new Tensor("grad", destGrad);
                destGradFeature.attachedTo = new Tuple2<>(ElementType.VERTEX, edge.dest.getId());
                destGradFeature.setTimestamp(destFeature.getTimestamp());
                new RemoteInvoke()
                        .toElement("trainer", ElementType.PLUGIN)
                        .noUpdate()
                        .withArgs(destGradFeature)
                        .withTimestamp(destGradFeature.getTimestamp())
                        .addDestination(edge.dest.masterPart())
                        .method("backward")
                        .where(MessageDirection.BACKWARD)
                        .buildAndRun(storage);
            }
            srcFeature.getValue().setRequiresGradient(false);
            destFeature.getValue().setRequiresGradient(false);
        }
        else {
            System.out.format("Backward failed since version out-of-date error %s", inference.ACTIVE);
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
        assert getPartId() == 0;
        inference.ACTIVE = false;
        removeFeaturesOnNextWatermark = true;
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
        removeFeaturesOnNextWatermark = true;
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
        }
    }

    /**
     * Given new parameters synchronize them across the parallel instances
     * @param params Parameters for this model
     */
    @RemoteFunction
    public void updateParameters(Map<String, NDArray> params) {
        inference.parameterStore.updateParameters(params);
        inference.parameterStore.resetGrads();
    }

    @Override
    public void onWatermark(long timestamp) {
        super.onWatermark(timestamp);
        if(removeFeaturesOnNextWatermark){
            if(getPartId() == replicaParts().get(replicaParts().size() - 1)){
                removeFeaturesOnNextWatermark = false;
                inference.ACTIVE = true;
            }
            List<Vertex> vertices = new ArrayList<>();
            for(Vertex v: storage.getVertices()){
                List<Edge> edges = new ArrayList<Edge>();
                storage.getIncidentEdges(v, EdgeType.BOTH).forEach(edges::add);
                edges.forEach(Edge::delete);
                vertices.add(v);
            }
            vertices.forEach(Vertex::deleteElement);
        }
    }
}
