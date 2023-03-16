package plugins.scheduler;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
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
    protected int trainingDataSize = 0;

    public BatchSizeTrainingScheduler(int batchSize){
        super("training_scheduler");
        Preconditions.checkState(batchSize > 0, "Batch size cannot be negative");
        this.batchSize = batchSize;
    }

    public BatchSizeTrainingScheduler(){
        this( 512);
    }


    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            if(++trainingDataSize == batchSize){
                getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingRequest());
            }
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.EnteredTraining){
            getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingSettingsRequest(trainingDataSize));
            trainingDataSize = 0;
        }
    }
}
