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
public class DatasetFinishTrainingScheduler extends Plugin {

    /**
     * Count of available training data up till now
     */
    protected static ThreadLocal<Integer> trainingDataSize = ThreadLocal.withInitial(()-> 0);

    public DatasetFinishTrainingScheduler(){
        super("training_scheduler");
    }

    @Override
    public void updateCurrentEffectiveWatermark(long watermark) {
        super.updateCurrentEffectiveWatermark(watermark);
        if(watermark == Long.MAX_VALUE) getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.RequestTraining());
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            int dataCount = trainingDataSize.get() + 1;
            trainingDataSize.set(dataCount);
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.FlushForTraining){
            getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.RequestMiniBatch(trainingDataSize.get()));
            trainingDataSize.set(0);
        }
    }
}
