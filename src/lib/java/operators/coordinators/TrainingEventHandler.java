package operators.coordinators;

import operators.events.StartTraining;
import operators.events.StopTraining;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.List;

@Deprecated
public class TrainingEventHandler implements WrapperOperatorEventHandler {
    private static final int modelUpdatedMessagesReceived = 0; // Message received to notify about model triggerUpdate. shared by all parallel handlers
    private static final int trainingMessagesReceived = 0; // Message received to start training
    private transient WrapperOperatorCoordinator mainCoordinator;

    public TrainingEventHandler() {

    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {

    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {

    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {

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
    public List<Class<? extends OperatorEvent>> getEventClasses() {
        return List.of(StartTraining.class, StopTraining.class);
    }


    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }
}
