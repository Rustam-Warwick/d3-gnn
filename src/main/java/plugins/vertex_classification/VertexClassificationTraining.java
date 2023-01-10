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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.GraphStorage;

import java.util.Arrays;
import java.util.Map;

/**
 * Does the last layer training for vertex classification based models
 */
public class VertexClassificationTraining extends BaseVertexOutput {

    /**
     * Loss function for gradient calculation
     */
    public final Loss loss;

    /**
     * Epoch Throughput counter
     */
    protected static transient ThreadLocal<Counter> epochThroughput = ThreadLocal.withInitial(SimpleCounter::new);

    /**
     * Epoch and MiniBatch Counter
     */
    protected static transient ThreadLocal<EpochAndMiniBatchController> epochAndMiniBatchControllers = ThreadLocal.withInitial(EpochAndMiniBatchController::new);

    /**
     * Map of training vertex map 2 part
     */
    protected transient Map<Short, ObjectArrayList<String>> part2TrainingVertexMap;


    public VertexClassificationTraining(String modelName, Loss loss) {
        super(modelName, "trainer");
        this.loss = loss;
    }

    @Override
    public synchronized void open(Configuration params) throws Exception {
        super.open(params);
        part2TrainingVertexMap = part2TrainingVertexMap == null?new Short2ObjectOpenHashMap<>():part2TrainingVertexMap;
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2TrainingVertexMap.put(part, new ObjectArrayList<>()));
        getRuntimeContext().getMetricGroup().meter("epochThroughput", new MeterView(epochThroughput.get(), 30));
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            part2TrainingVertexMap.get(getPart()).add((String) ((Feature<?, ?>) element).getAttachedElementId());
        }
    }

    /**
     * For all the trainVertices compute the backward pass and send the collected gradients to previous layer
     * <p>
     * Only works with Master vertices of this storage layer which have train labels.
     * Because of flushing before training assumes that all vertices have their features in place here.
     * </p>
     */
    public void startTraining() {
        // 1. Compute the gradients per each vertex output feature
        ObjectArrayList<String> vertexIds = part2TrainingVertexMap.get(getPart());
        int[] startEndIndices = epochAndMiniBatchControllers.get().getStartEndIndices(vertexIds.size());
        if(startEndIndices[0] >= startEndIndices[1]) return; // No data available
        String[] miniBatchVertexIds = new String[startEndIndices[1] - startEndIndices[0]];
        System.arraycopy(vertexIds.elements(), startEndIndices[0], miniBatchVertexIds, 0, miniBatchVertexIds.length);
        Arrays.sort(miniBatchVertexIds);
        NDList inputs = new NDList(miniBatchVertexIds.length);
        NDList labels = new NDList(miniBatchVertexIds.length);
        Tuple3<ElementType, Object, String> reuse = Tuple3.of(ElementType.VERTEX, null, "f");
        Tuple3<ElementType, Object, String> reuse2 = Tuple3.of(ElementType.VERTEX, null, "tl");
        for (String vertexId : miniBatchVertexIds) {
            reuse.f1 = vertexId;
            reuse2.f1 = vertexId;
            inputs.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuse).getValue());
            labels.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuse2).getValue());
        }
        NDList batchedInputs = new NDList(NDArrays.stack(inputs));
        batchedInputs.get(0).setRequiresGradient(true);
        NDList batchedLabels = new NDList(NDArrays.stack(labels));
        NDList predictions = output(batchedInputs, true);
        NDArray meanLoss = loss.evaluate(batchedLabels, predictions);
        synchronized(this) {
            // Synchronize the backward call
            JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) BaseNDManager.getManager().ones(new Shape()), false, false);
        }
        NDArray batchedGradients = batchedInputs.get(0).getGradient();
        Rmi.buildAndRun(
                getId(),
                getType(),
                "collect",
                getPart(),
                OutputTags.BACKWARD_OUTPUT_TAG,
                miniBatchVertexIds,
                batchedGradients
        );
        // Do this to clear up
        BaseNDManager.getManager().resumeAndDelay();
    }

    /**
     * Controller based on EPOCH and Mini-Batch of training data
     */
    public static class EpochAndMiniBatchController {

        /**
         * Current miniBatch that is being processed
         */
        short currentMiniBatch;

        /**
         * Current epoch that is being processed
         */
        short currentEpoch;

        /**
         * Total number of miniBatches to process
         */
        short miniBatches;

        /**
         * Total number of Epochs
         */
        short epochs;

        /**
         * Update the miniBatch Count and Epochs
         */
        public void setMiniBatchAndEpochs(short miniBatches, short epochs) {
            this.miniBatches = miniBatches;
            this.epochs = epochs;
        }

        /**
         * Finish a mini-batch and check if more are remaining
         */
        public boolean miniBatchFinishedCheckIfMore(){
          currentMiniBatch = (short) ((currentMiniBatch + 1) % miniBatches);
          if(currentMiniBatch == 0) currentEpoch++;
          if(currentEpoch < epochs){
              currentEpoch = 0;
              return true;
          }
          return false;
        }

        /**
         * Returns the indices starting from 0 for this miniBatch iteration
         * [start_index, end_index)
         */
        public int[] getStartEndIndices(int datasetSize){
            int miniBatchSize = (int) Math.ceil(((double) datasetSize / miniBatches));
            int startIndex = currentMiniBatch * miniBatchSize;
            int endIndex = Math.min(datasetSize, startIndex + miniBatchSize);
            return new int[]{startIndex, endIndex};
        }

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
        if(evt instanceof TrainingSubCoordinator.StartTraining){
            epochAndMiniBatchControllers.get().setMiniBatchAndEpochs(((TrainingSubCoordinator.StartTraining) evt).miniBatches, ((TrainingSubCoordinator.StartTraining) evt).epochs);
            try(GraphStorage.ReuseScope ignored = getRuntimeContext().getStorage().withReuse()) {getRuntimeContext().runForAllLocalParts(this::startTraining);}
            getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.BackwardPhaser()), OutputTags.BACKWARD_OUTPUT_TAG);
        }
        else if(evt instanceof TrainingSubCoordinator.ForwardPhaser && ((TrainingSubCoordinator.ForwardPhaser) evt).iteration == 1){
            if(epochAndMiniBatchControllers.get().miniBatchFinishedCheckIfMore()){
                // Has more
                getRuntimeContext().runForAllLocalParts(this::startTraining);
                getRuntimeContext().broadcast(new GraphOp(new TrainingSubCoordinator.BackwardPhaser()), OutputTags.BACKWARD_OUTPUT_TAG);
            }else{
                // Stop training
                System.out.println("Finished Training");
            }
        }
    }
}
