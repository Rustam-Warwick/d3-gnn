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
import org.apache.flink.runtime.state.tmshared.TMSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.BaseStorage;

import java.util.Map;

/**
 * Does the last layer training for vertex classification based models
 */
public class BinaryVertexClassificationTraining extends BaseVertexOutput {

    /**
     * Loss functions for gradient calculation
     */
    public final Loss loss;

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

    public BinaryVertexClassificationTraining(String modelName, Loss loss) {
        super(modelName, "trainer");
        this.loss = loss;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        reuseLabelsNDList = new NDList();
        reuseFeaturesNDList = new NDList();
        reuseFeaturesId = Tuple3.of(ElementType.VERTEX, null, "f");
        reuseLabelsId = Tuple3.of(ElementType.VERTEX, null, "tl");
        part2TrainingVertexMap = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("part2GradientAgg", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        synchronized (part2TrainingVertexMap) {
            getRuntimeContext().getThisOperatorParts().forEach(part -> part2TrainingVertexMap.put(part, new ObjectArrayList<>()));
        }
        miniBatchEpochController = new MiniBatchEpochController();
        epochThroughput = new SimpleCounter();
        getRuntimeContext().getMetricGroup().meter("epochThroughput", new MeterView(epochThroughput, 30));
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?, ?>) element).getName().equals("tl")) {
            part2TrainingVertexMap.get(getPart()).add((String) ((Feature<?, ?>) element).getAttachedElementId());
            collectedTrainingDataCount++;
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
        // 1. Prepare data
        ObjectArrayList<String> vertexIds = part2TrainingVertexMap.get(getPart());
        int[] startEndIndices = miniBatchEpochController.getStartEndIndices(vertexIds.size());
        if (startEndIndices[0] >= startEndIndices[1]) return; // No data available for this part.
        String[] miniBatchVertexIds = new String[startEndIndices[1] - startEndIndices[0]];
        System.arraycopy(vertexIds.elements(), startEndIndices[0], miniBatchVertexIds, 0, miniBatchVertexIds.length);
        reuseFeaturesNDList.clear();
        reuseLabelsNDList.clear();

        // 2. Collect data from storage
        try (BaseStorage.ObjectPoolScope scope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (String vertexId : miniBatchVertexIds) {
                reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, vertexId, "f").getValue());
                reuseLabelsNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, vertexId, "tl").getValue());
                scope.refresh();
            }
        }

        // 3. Bakward pass
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
        BaseNDManager.getManager().clean(); // Refresh
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
            epochThroughput.inc();
            LOG.error(String.format("Start training batch %s epoch %s", ((TrainingSubCoordinator.StartTrainingWithSettings) evt).miniBatches, ((TrainingSubCoordinator.StartTrainingWithSettings) evt).epochs));
            miniBatchEpochController.setMiniBatchAndEpochs(((TrainingSubCoordinator.StartTrainingWithSettings) evt).miniBatches, ((TrainingSubCoordinator.StartTrainingWithSettings) evt).epochs);
            getRuntimeContext().runForAllLocalParts(this::startTraining);
            getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.BackwardPhaser()), OutputTags.BACKWARD_OUTPUT_TAG);
        } else if (evt instanceof TrainingSubCoordinator.ForwardPhaser && ((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 2) {
            epochThroughput.inc();
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
        } else if (evt instanceof TrainingSubCoordinator.EnteredTraining) {
            getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingSettingsRequest(collectedTrainingDataCount));
        }
    }

}
