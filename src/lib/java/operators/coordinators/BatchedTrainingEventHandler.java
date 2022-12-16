package operators.coordinators;


import operators.events.FinalWatermarkArrived;
import operators.events.ForwardBarrier;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class BatchedTrainingEventHandler implements WrapperOperatorEventHandler {

    private int finalWatermarksCount;

    private WrapperOperatorCoordinator mainCoordinator;

    @Override
    public WrapperOperatorCoordinator getCoordinator() {
        return mainCoordinator;
    }

    @Override
    public void setCoordinator(WrapperOperatorCoordinator coordinator) {
        mainCoordinator = coordinator;
    }

    @Override
    public List<Class<? extends OperatorEvent>> getEventClasses() {
        return List.of(FinalWatermarkArrived.class);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    public void handleFinalWatermarkEvent() {
        if (mainCoordinator.position >= mainCoordinator.layers) {
            if (++finalWatermarksCount >= WrapperOperatorCoordinator.subtaskGateways.get(mainCoordinator.position).length) {
                // All have been received
                ForwardBarrier evt = new ForwardBarrier();
                for (SubtaskGateway subtaskGateway : WrapperOperatorCoordinator.subtaskGateways.get((short) 1)) {
                    subtaskGateway.sendEvent(evt);
                }
            }
        }
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {

        if (event instanceof FinalWatermarkArrived) handleFinalWatermarkEvent();
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {

    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }
}
