package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageCommunication;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import elements.iterations.Rmi;
import operators.events.StartTraining;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexClassificationTrainingPlugin extends Plugin {

    public final String modelName;

    public final int MAX_BATCH_SIZE; // Overall batch size across output layers
    public final SerializableLoss loss;
    public int LOCAL_BATCH_SIZE; // Estimate batch size for this operator
    public transient VertexOutputLayer outputLayer;

    public transient StackBatchifier batchifier; // Helper for batching data

    public int BATCH_COUNT = 0;


    public VertexClassificationTrainingPlugin(String modelName, SerializableLoss loss, int MAX_BATCH_SIZE) {
        super(String.format("%s-trainer", modelName));
        this.MAX_BATCH_SIZE = MAX_BATCH_SIZE;
        this.loss = loss;
        this.modelName = modelName;
    }

    public VertexClassificationTrainingPlugin(String modelName, SerializableLoss loss) {
        this(modelName, loss, 1024);
    }

    @Override
    public void open() throws Exception {
        super.open();
        outputLayer = (VertexOutputLayer) storage.getPlugin(String.format("%s-inferencer", modelName));
        LOCAL_BATCH_SIZE = MAX_BATCH_SIZE / storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
        batchifier = new StackBatchifier();
    }
    // INITIALIZATION DONE!!!

    /**
     * Add value to the batch. If filled send event to the coordinator
     */
    public void incrementBatchCount() {
        BATCH_COUNT++;
        if (BATCH_COUNT % LOCAL_BATCH_SIZE == 0) {
            storage.layerFunction.operatorEventMessage(new StartTraining());
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && ("feature".equals(feature.getName()) || "trainLabel".equals(feature.getName())) && feature.getElement() != null) {
                if (isTrainReady((Vertex) feature.getElement())) {
                    incrementBatchCount();
                    Feature<List<String>, List<String>> trainVertices = (Feature<List<String>, List<String>>) getFeature("trainVertices");
                    if (trainVertices == null) {
                        List<String> tmp = new ArrayList<>();
                        tmp.add(feature.getElement().getId());
                        setFeature("trainVertices", new Feature<>(tmp, true, getPartId()));
                    } else {
                        trainVertices.getValue().add(feature.getElement().getId());
                        storage.updateFeature(trainVertices);
                    }
                }
            }
        }
    }

    /**
     * Both feature and label are here
     *
     * @param v Vertex to check for
     */
    public boolean isTrainReady(Vertex v) {
        return v.getFeature("trainLabel") != null && v.getFeature("feature") != null;
    }

    /**
     * For all the trainVertices compute the backward pass and send it to the previous layer
     * After sending it, send an acknowledgement to all of the previous operators tob= notify the continuation
     */
    public void startTraining() {
        Feature<List<String>, List<String>> trainVertices = (Feature<List<String>, List<String>>) getFeature("trainVertices");
        if (trainVertices != null && !trainVertices.getValue().isEmpty()) {
            // 1. Compute the gradients per each vertex output feature
            List<NDList> inputs = new ArrayList<>();
            List<NDList> labels = new ArrayList<>();
            try {
                for (String vId : trainVertices.getValue()) {
                    Vertex v = storage.getVertex(vId);
                    ((NDArray) v.getFeature("feature").getValue()).setRequiresGradient(true);
                    inputs.add(new NDList((NDArray) v.getFeature("feature").getValue()));
                    labels.add(new NDList((NDArray) v.getFeature("trainLabel").getValue()));
                    v.getFeature("trainLabel").delete(); // Delete so it does not retrigger it
                }
                NDList batchedInputs = batchifier.batchify(inputs.toArray(NDList[]::new));
                NDList batchedLabels = batchifier.batchify(labels.toArray(NDList[]::new));
                NDList predictions = outputLayer.output(batchedInputs, true);
                NDArray meanLoss = loss.evaluate(batchedLabels, predictions);
                JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) LifeCycleNDManager.getInstance().ones(new Shape()), false, false);

                // 2. Prepare the HashMap for Each Vertex and send to previous layer
                HashMap<String, NDArray> backwardGrads = new HashMap<>();
                for (int i = 0; i < trainVertices.getValue().size(); i++) {
                    backwardGrads.put(trainVertices.getValue().get(i), inputs.get(i).get(0).getGradient());
                }
                new RemoteInvoke()
                        .addDestination(getPartId()) // Only masters will be here anyway
                        .noUpdate()
                        .method("collect")
                        .toElement(getId(), elementType())
                        .where(MessageDirection.BACKWARD)
                        .withArgs(backwardGrads)
                        .buildAndRun(storage);
            } catch (Exception e) {
                // Pass
            } finally {
                //3. Clean the trainVertices data and clean the inputs
                inputs.forEach(item -> item.get(0).setRequiresGradient(false));
                trainVertices.getValue().clear();
                storage.updateFeature(trainVertices);
            }
        }
        if (isLastReplica()) {
            // This is the last call of plugin from this operator so send ack message to previous operator
            BATCH_COUNT = 0;
            Rmi synchronize = new Rmi(getId(), "synchronize", new Object[]{}, elementType(), false, null);
            storage.layerFunction.broadcastMessage(new GraphOp(Op.RMI, null, synchronize, null, MessageCommunication.BROADCAST), MessageDirection.BACKWARD);
            outputLayer.modelServer.getParameterStore().sync();

        }
    }

    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof StartTraining) {
            if (isLastReplica())
                System.out.format("Start training %s\n", storage.layerFunction.getRuntimeContext().getIndexOfThisSubtask());
            startTraining();
        }
    }

}
