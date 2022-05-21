package operators.coordinators;

import operators.coordinators.events.StartTraining;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.List;

public class StartTrainingEventHandler implements WrapperOperatorEventHandler {
    private transient WrapperOperatorCoordinator mainCoordinator;
    private int trainingMessageReceived;

    public StartTrainingEventHandler(){
        trainingMessageReceived = 0;
    }

    @Override
    public WrapperOperatorCoordinator getCoordinator() {
        return mainCoordinator;
    }

    @Override
    public void setCoordinator(WrapperOperatorCoordinator coordinator) {
        this.mainCoordinator = coordinator;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        if(getCoordinator().position == getCoordinator().layers){
            // This is the last layer
            trainingMessageReceived++;
            if(trainingMessageReceived == getCoordinator().context.currentParallelism()){
                SubtaskGateway[] layerZeroGateways = WrapperOperatorCoordinator.subtaskGateways.get((short) 0);
                for(SubtaskGateway e: layerZeroGateways){
                    e.sendEvent(event); // Send start training event
                }
            }
        }
    }

    @Override
    public List<Class<? extends OperatorEvent>> getEventClasses() {
        return List.of(StartTraining.class);
    }


    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {

    }
}
