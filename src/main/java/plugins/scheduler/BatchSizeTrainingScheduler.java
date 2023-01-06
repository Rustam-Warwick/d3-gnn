package plugins.scheduler;

import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.enums.ElementType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;

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
    protected transient ThreadLocal<Integer> trainingDataCount;

    public BatchSizeTrainingScheduler(String modelName, int batchSize){
        super("training_scheduler");
        this.batchSize = batchSize;
    }

    public BatchSizeTrainingScheduler(String modelName){
        this(modelName, 512);
    }

    @Override
    public synchronized void open(Configuration parameters) throws Exception {
        super.open(parameters);
        trainingDataCount = trainingDataCount == null ? new ThreadLocal<>(): trainingDataCount;
        trainingDataCount.set(0);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.getType() == ElementType.ATTACHED_FEATURE && ((Feature<?,?>)element).getName().equals("tl")){
            int dataCount = trainingDataCount.get() + 1;
            trainingDataCount.set(dataCount);
            if(dataCount == batchSize){
                getRuntimeContext().sendOperatorEvent(new TrainingSubCoordinator.RequestTraining());
            }
        }
    }
}
