package plugins.vertex_classification;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.training.loss.Loss;
import elements.Feature;
import elements.GraphElement;
import elements.GraphOp;
import elements.Rmi;
import elements.enums.ElementType;
import helpers.MiniBatchEpochController;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.taskshared.TaskSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.BaseStorage;

import java.util.Map;

/**
 * Does the last layer training for vertex classification based models
 */
public class BatchSizeBinaryVertexClassificationTraining extends BaseVertexOutput {

    /**
     * Loss functions for gradient calculation
     */
    public final Loss loss;

    /**
     * Batch size for training to start
     */
    protected final int batchSize;

    /**
     * Size of collected data in this plugin over all parts represented
     */
    protected transient int collectedTrainingDataCount;

    /**
     * Epoch Throughput counter
     */
    protected transient Counter epochThroughput;

    /**
     * Epoch and MiniBatch Counter
     */
    protected transient MiniBatchEpochController miniBatchEpochController;

    /**
     * Map of training vertex map 2 part
     */
    protected transient Map<Short, ObjectArrayList<String>> part2TrainingVertexMap;

    /**
     * Reuse for inputs NDList
     */
    protected transient NDList reuseFeaturesNDList;

    /**
     * Reuse for labels NDList
     */
    protected transient NDList reuseLabelsNDList;

    /**
     * Reuse for feature id
     */
    protected transient Tuple3<ElementType, Object, String> reuseFeaturesId;

    /**
     * Reuse for labels id
     */
    protected transient Tuple3<ElementType, Object, String> reuseLabelsId;

    public BatchSizeBinaryVertexClassificationTraining(String modelName, Loss loss, int batchSize) {
        super(modelName, "trainer");
        this.loss = loss;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseLabelsNDList = new NDList();
        reuseFeaturesNDList = new NDList();
        reuseFeaturesId = Tuple3.of(ElementType.VERTEX, null, "f");
        reuseLabelsId = Tuple3.of(ElementType.VERTEX, null, "tl");
        part2TrainingVertexMap = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("part2GradientAgg", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2TrainingVertexMap.put(part, new ObjectArrayList<>()));
        miniBatchEpochController = new MiniBatchEpochController();
        epochThroughput = new SimpleCounter();
        getRuntimeContext().getMetricGroup().meter("epochThroughput", new MeterView(epochThroughput, 30));
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?, ?>) element).getName().equals("tl")) {
            part2TrainingVertexMap.get(getPart()).add((String) ((Feature<?, ?>) element).getAttachedElementId());
            if (++collectedTrainingDataCount == batchSize) {
                getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingRequest());
            }
        }
    }

    /**
     * For all the trainVertices compute the backward pass and send the collected gradients to previous layer
     * <p>
     *     <ul>
     *         <li>Assumes all the added vertices have their up-to-date features in place</li>
     *     </ul>
     * </p>
     */
    public void startTraining() {
        // 1. Compute the gradients per each vertex output feature
        ObjectArrayList<String> vertexIds = part2TrainingVertexMap.get(getPart());
        int[] startEndIndices = miniBatchEpochController.getStartEndIndices(vertexIds.size());
        if (startEndIndices[0] >= startEndIndices[1]) return; // No data available for this part.
        String[] miniBatchVertexIds = new String[startEndIndices[1] - startEndIndices[0]];
        System.arraycopy(vertexIds.elements(), startEndIndices[0], miniBatchVertexIds, 0, miniBatchVertexIds.length);
        reuseFeaturesNDList.clear();
        reuseLabelsNDList.clear();
        for (String vertexId : miniBatchVertexIds) {
            try(BaseStorage.ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
                reuseFeaturesId.f1 = reuseLabelsId.f1= vertexId;
                reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuseFeaturesId).getValue());
                reuseLabelsNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuseLabelsId).getValue());
            }
        }
        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        NDArray batchedLabels = NDArrays.stack(reuseLabelsNDList);
        batchedFeatures.setRequiresGradient(true);
        reuseFeaturesNDList.clear();
        reuseLabelsNDList.clear();
        reuseFeaturesNDList.add(batchedFeatures);
        reuseLabelsNDList.add(batchedLabels);
        NDList predictions = output(reuseFeaturesNDList, true);
        NDArray meanLoss = loss.evaluate(reuseLabelsNDList, predictions);
        System.out.format("Accuracy %s\n", batchedLabels.eq(predictions.get(0).flatten().gt(0.5)).sum().getLong() / (double) batchedLabels.size());
        synchronized (modelServer.getModel()) {
            // Synchronize the backward call
            JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) BaseNDManager.getManager().ones(new Shape()), false, false);
        }
        NDArray batchedGradients = batchedFeatures.getGradient();
        Rmi.buildAndRun(
                getId(),
                getType(),
                "collect",
                getPart(),
                OutputTags.BACKWARD_OUTPUT_TAG,
                miniBatchVertexIds,
                batchedGradients
        );
        BaseNDManager.getManager().resumeAndDelay(); // Refresh
    }

    /**
     * Handle Training Start
     * <p>
     * ForwardBarrier -> Send Training Grads back, Send BackwardBarrier backwards, Call for model sync
     * </p>
     */
    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof TrainingSubCoordinator.StartTrainingWithSettings) {
            // Adjust the minibatch and epoch count, do the backward pass
            miniBatchEpochController.setMiniBatchAndEpochs(((TrainingSubCoordinator.StartTrainingWithSettings) evt).miniBatches, ((TrainingSubCoordinator.StartTrainingWithSettings) evt).epochs);
            getRuntimeContext().runForAllLocalParts(this::startTraining);
            getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.BackwardPhaser()), OutputTags.BACKWARD_OUTPUT_TAG);
        } else if (evt instanceof TrainingSubCoordinator.ForwardPhaser && ((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 2) {
            if (miniBatchEpochController.miniBatchFinishedCheckIfMore()) {
                // Has more
                getRuntimeContext().runForAllLocalParts(this::startTraining);
                getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.BackwardPhaser()), OutputTags.BACKWARD_OUTPUT_TAG);
            } else {
                // Stop training
                miniBatchEpochController.clear();
                collectedTrainingDataCount = 0;
                getRuntimeContext().runForAllLocalParts(() -> part2TrainingVertexMap.get(getPart()).clear());
                getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.ExitedTraining()), OutputTags.BACKWARD_OUTPUT_TAG);
            }
        } else if(evt instanceof TrainingSubCoordinator.EnteredTraining){
            getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingSettingsRequest(collectedTrainingDataCount));
        }
    }

}
