package plugins.scheduler;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import org.apache.flink.util.Preconditions;

/**
 * Plugin to Request training once the batch size is filled
 * <p>
 *     Assumes that if label is received data is already here or will be once flushed
 * </p>
 */
public class BatchSizeTrainingScheduler extends Plugin {

    /**
     * Once the batch size is filled send request to coordinator
     */
    protected final int batchSize;

    /**
     * Count of available training data up till now
     */
    protected transient ThreadLocal<Integer> trainingDataSize;

    public BatchSizeTrainingScheduler(int batchSize){
        super("training_scheduler");
        Preconditions.checkState(batchSize > 0, "Batch size cannot be negative");
        this.batchSize = batchSize;
    }

    public BatchSizeTrainingScheduler(){
        this( 512);
    }

    @Override
    public synchronized void open(Configuration parameters) throws Exception {
        super.open(parameters);
        trainingDataSize = trainingDataSize == null ? ThreadLocal.withInitial(() -> 0): trainingDataSize;
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            int dataCount = trainingDataSize.get() + 1;
            trainingDataSize.set(dataCount);
            if(dataCount == batchSize){
                getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.RequestTraining());
            }
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.FlushForTraining){
            getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.RequestMiniBatch(trainingDataSize.get()));
        }
    }
}
