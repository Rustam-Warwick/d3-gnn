package plugins.vertex_classification;

import ai.djl.ndarray.NDArrayCollector;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.training.loss.Loss;
import elements.GraphOp;
import elements.ReplicaState;
import elements.Vertex;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import operators.events.BackwardBarrier;
import operators.events.BaseOperatorEvent;
import operators.events.ForwardBarrier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexClassificationTrainingPlugin extends BaseVertexOutputPlugin {

    public final Loss loss;

    public VertexClassificationTrainingPlugin(String modelName, Loss loss) {
        super(modelName, "trainer");
        this.loss = loss;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    /**
     * For all the trainVertices compute the backward pass and send the collected gradients to previous layer
     * <p>
     *      Only works with Master vertices of this storage layer which have train labels.
     *      Because of flushing before training assumes that all vertices have their features in place here.
     * </p>
     */
    public void startTraining() {
        // 1. Compute the gradients per each vertex output feature
        NDList inputs = new NDList();
        NDList labels = new NDList();
        List<String> vertexIds = new ArrayList<>();
        for (Vertex vertex : storage.getVertices()) {
            if (vertex.state() != ReplicaState.MASTER) continue;
            inputs.add((NDArray) vertex.getFeature("f").getValue());
            labels.add((NDArray) vertex.getFeature("train_l").getValue());
            vertexIds.add(vertex.getId());
        }
        if(inputs.isEmpty()) return;
        NDList batchedInputs = new NDList(NDArrays.stack(inputs));
        batchedInputs.get(0).setRequiresGradient(true);
        NDList batchedLabels = new NDList(NDArrays.stack(labels));
        NDList predictions = output(batchedInputs, true);
        NDArray meanLoss = loss.evaluate(batchedLabels, predictions);
        System.out.println(meanLoss);
        JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) LifeCycleNDManager.getInstance().ones(new Shape()), false, false);
        NDArray gradient = batchedInputs.get(0).getGradient();
        // 2. Prepare the HashMap for Each Vertex and send to previous layer
        HashMap<String, NDArray> backwardGrads = new NDArrayCollector<>(false);
        for (int i = 0; i < vertexIds.size(); i++) {
            backwardGrads.put(vertexIds.get(i), gradient.get(i));
        }
        new RemoteInvoke()
                .addDestination(getPartId()) // Only masters will be here anyway
                .noUpdate()
                .method("collect")
                .toElement(getId(), elementType())
                .where(MessageDirection.BACKWARD)
                .withArgs(backwardGrads)
                .buildAndRun(storage);

    }

    /**
     * Handle Training Start
     * <p>
     *     ForwardBarrier -> Send Training Grads back, Send BackwardBarrier backwards, Call for model sync
     * </p>
     */
    @Override
    public void onOperatorEvent(BaseOperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof ForwardBarrier) {
            storage.layerFunction.runForAllLocalParts(this::startTraining);
            storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.BACKWARD)), MessageDirection.BACKWARD);
            modelServer.getParameterStore().sync();
        }
    }

}
