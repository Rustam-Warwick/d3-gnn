package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.GraphOp;
import elements.ReplicaState;
import elements.Vertex;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import helpers.GradientCollector;
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

    public final SerializableLoss loss;

    public VertexClassificationTrainingPlugin(String modelName, SerializableLoss loss) {
        super(modelName, "trainer");
        this.loss = loss;
    }


    @Override
    public void open() throws Exception {
        super.open();
    }

    /**
     * Both feature and label are here
     *
     * @param v Vertex to check for
     */
    public boolean isTrainReady(Vertex v) {
        return v.getFeature("train_l") != null && v.getFeature("f") != null;
    }

    /**
     * For all the trainVertices compute the backward pass and send it to the previous layer
     * After sending it, send an acknowledgement to all the previous operators tob= notify the continuation
     */
    public void startTraining() {
        // 1. Compute the gradients per each vertex output feature
        NDList inputs = new NDList();
        NDList labels = new NDList();
        List<String> trainVertices = new ArrayList<>();
        for (Vertex vertex : storage.getVertices()) {
            if (vertex.state() != ReplicaState.MASTER || !vertex.containsFeature("f") || !vertex.containsFeature("train_l")) continue;
            inputs.add((NDArray) vertex.getFeature("f").getValue());
            labels.add((NDArray) vertex.getFeature("train_l").getValue());
            trainVertices.add(vertex.getId());
        }
        if (!inputs.isEmpty()) {
            NDList batchedInputs = new NDList(NDArrays.stack(inputs));
            batchedInputs.get(0).setRequiresGradient(true);
            NDList batchedLabels = new NDList(NDArrays.stack(labels));
            NDList predictions = output(batchedInputs, true);
            NDArray meanLoss = loss.internalLoss.evaluate(batchedLabels, predictions);
            JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) LifeCycleNDManager.getInstance().ones(new Shape()), false, false);
            NDArray gradient = batchedInputs.get(0).getGradient();
            // 2. Prepare the HashMap for Each Vertex and send to previous layer
            HashMap<String, NDArray> backwardGrads = new GradientCollector<>();
            for (int i = 0; i < trainVertices.size(); i++) {
                backwardGrads.put(trainVertices.get(i), gradient.get(i));
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

    }

    @Override
    public void onOperatorEvent(BaseOperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof ForwardBarrier) {
            storage.layerFunction.runForAllLocalParts(this::startTraining);
            storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.BACKWARD)), MessageDirection.BACKWARD);
            modelServer.getParameterStore().sync();

        }
//        if (event instanceof StartTraining) {
//            if (isLastReplica())
//                System.out.format("Start training %s\n", storage.layerFunction.getRuntimeContext().getIndexOfThisSubtask());
//            startTraining();
//        }
    }

}
