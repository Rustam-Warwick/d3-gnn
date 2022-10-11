package operators.coordinators;

import operators.events.StartTraining;
import operators.events.StopTraining;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.List;

public class TrainingEventHandler implements WrapperOperatorEventHandler {
    private static final int modelUpdatedMessagesReceived = 0; // Message received to notify about model update. shared by all parallel handlers

    private static int trainingMessagesReceived = 0; // Message received to start training

    private transient WrapperOperatorCoordinator mainCoordinator;

    public TrainingEventHandler() {

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
        if (event instanceof StartTraining) {
            if (getCoordinator().position == getCoordinator().layers) {
                // This is the last layer
                trainingMessagesReceived++;
                if (trainingMessagesReceived == getCoordinator().context.currentParallelism()) {
                    SubtaskGateway[] layerZeroGateways = WrapperOperatorCoordinator.subtaskGateways.get((short) 0);
                    for (SubtaskGateway e : layerZeroGateways) {
                        e.sendEvent(event); // Send start training event to the 0 operator
                    }
                }
            }
        }

        if (event instanceof StopTraining) {
            if (getCoordinator().position == getCoordinator().layers) {
                // This is the last layer
                trainingMessagesReceived--;
                if (trainingMessagesReceived == 0) {
                    SubtaskGateway[] layerZeroGateways = WrapperOperatorCoordinator.subtaskGateways.get((short) 0);
                    for (SubtaskGateway e : layerZeroGateways) {
                        e.sendEvent(event); // Send start training event to the 0 operator
                    }
                }
            }
        }
    }

    @Override
    public List<Class<? extends OperatorEvent>> getEventClasses() {
        return List.of(StartTraining.class, StopTraining.class);
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
