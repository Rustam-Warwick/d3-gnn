package plugins.scheduler;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;

/**
 * Plugin to Request training once the batch size is filled
 * <p>
 *     Assumes that if label is received data is already here or will be once flushed
 * </p>
 */
public class DatasetFinishTrainingScheduler extends Plugin {

    /**
     * Count of available training data up till now
     */
    protected int trainingDataSize = 0;

    public DatasetFinishTrainingScheduler(){
        super("training_scheduler");
    }

    @Override
    public void updateCurrentEffectiveWatermark(long watermark) {
        super.updateCurrentEffectiveWatermark(watermark);
        if(watermark == Long.MAX_VALUE) getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.TrainingRequest());
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            trainingDataSize++;
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
