package org.apache.flink.streaming.api.operators.graph;

import elements.GraphEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * SubCoordinator for handling the start and stop of the training loop
 */
public class TrainingSubCoordinator extends GraphOperatorCoordinator.GraphOperatorSubCoordinator {

    /**
     * How many messages should we receive to start training
     */
    protected final short numRequestEventsToStart;

    /**
     * How many requests events we have received so far
     */
    protected short receivedRequestEvents;

    public TrainingSubCoordinator(GraphOperatorCoordinator mainCoordinator, float percentOfRequestToStart){
        super(mainCoordinator);
        Preconditions.checkState(percentOfRequestToStart > 0 && percentOfRequestToStart <= 1, "Percent should be between (0 and 1]");
        this.numRequestEventsToStart = (short) (mainCoordinator.context.currentParallelism() * percentOfRequestToStart);
    }

    public TrainingSubCoordinator(GraphOperatorCoordinator mainCoordinator) {
        this(mainCoordinator, 0.3f);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if((event instanceof RequestTraining) && (++receivedRequestEvents == numRequestEventsToStart)){
            for (SubtaskGateway subTaskGateway : mainCoordinator.positionToCoordinators.get((short) 0).subTaskGateways) {
                subTaskGateway.sendEvent(new FlushDataFlow());
            }
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {

    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {

    }

    /**
     * Final layer requests this coordinator to start the training loop
     * Actual training loop is started once {@code numRequestEventsToStart} of such events are received
     */
    public static class RequestTraining implements OperatorEvent{}

    /**
     * Event that is sent to the {@link DatasetSplitterOperator} indicating that training is entering to flush phase
     * This event will further go down the pipeline until the last operator is met
     */
    public static class FlushDataFlow extends GraphEvent{

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {

        }
    }

}
