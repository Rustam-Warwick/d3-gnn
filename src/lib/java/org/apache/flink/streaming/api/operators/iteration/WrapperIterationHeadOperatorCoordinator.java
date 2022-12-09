package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinator for {@link WrapperIterationHeadOperator}
 * Similarly it optionally wraps around internal operators {@link OperatorCoordinator} but on top of it implements distributed termination detection
 */
public class WrapperIterationHeadOperatorCoordinator implements OperatorCoordinator {

    /**
     * Coordinator for the internal operator
     */
    @Nullable
    protected final OperatorCoordinator bodyOperatorCoordinator;

    /**
     * Operator Context
     */
    protected final Context context;

    public WrapperIterationHeadOperatorCoordinator(@Nullable  OperatorCoordinator bodyOperatorCoordinator, Context context) {
        this.bodyOperatorCoordinator = bodyOperatorCoordinator;
        this.context = context;
        context.getCoordinatorStore().compute("iteration_coordinators", (key, value) -> {
           if(value == null) return new ArrayList<>(List.of(this));
           else{
               ((List<Object>) value).add(this);
           }
           return value;
        });
    }

    public void

    @Override
    public void start() throws Exception {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.start();
    }

    @Override
    public void close() throws Exception {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.handleEventFromOperator(subtask, attemptNumber, event);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.checkpointCoordinator(checkpointId, resultFuture);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.executionAttemptFailed(subtask, attemptNumber, reason);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        if(bodyOperatorCoordinator != null) bodyOperatorCoordinator.executionAttemptReady(subtask, attemptNumber, gateway);
    }


    /**
     * Message for Termination Detection
     */
    public static class StartTermination implements OperatorEvent{}

    /**
     * Simple Provider implementation
     */
    public static class WrapperIterationHeadOperatorCoordinatorProvider implements OperatorCoordinator.Provider{

        protected final OperatorID operatorID;

        @Nullable
        protected final OperatorCoordinator.Provider bodyOperatorCoordinatorProvider;

        public WrapperIterationHeadOperatorCoordinatorProvider(OperatorID operatorID, OperatorCoordinator.@Nullable Provider bodyOperatorCoordinatorProvider) {
            this.operatorID = operatorID;
            this.bodyOperatorCoordinatorProvider = bodyOperatorCoordinatorProvider;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            if(bodyOperatorCoordinatorProvider == null) return new WrapperIterationHeadOperatorCoordinator(null, context);
            else return new WrapperIterationHeadOperatorCoordinator(bodyOperatorCoordinatorProvider.create(context), context);
        }
    }
}
