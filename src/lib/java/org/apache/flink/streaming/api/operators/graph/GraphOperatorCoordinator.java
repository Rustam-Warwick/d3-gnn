package org.apache.flink.streaming.api.operators.graph;

import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Operator Coordinator for {@link GraphStorageOperator} and {@link DatasetSplitterOperator}
 */
public class GraphOperatorCoordinator implements OperatorCoordinator {

    protected final Context context;

    protected final short position;

    protected Short2ObjectOpenHashMap<GraphOperatorCoordinator> positionToCoordinators;

    public GraphOperatorCoordinator(Context context, short position) {
        this.context = context;
        this.position = position;
        positionToCoordinators = (Short2ObjectOpenHashMap<GraphOperatorCoordinator>) context.getCoordinatorStore().compute("graph_coordinators", (key, val)->{
            if(val == null) val = new Short2ObjectOpenHashMap<>();
            ((Short2ObjectOpenHashMap<GraphOperatorCoordinator>) val).put(position, this);
            return val;
        });
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {

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

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        OperatorCoordinator.super.notifyCheckpointAborted(checkpointId);
    }


    public static class GraphOperatorCoordinatorProvider implements OperatorCoordinator.Provider{

        final protected short position;

        final protected OperatorID operatorID;

        public GraphOperatorCoordinatorProvider(short position, OperatorID operatorID) {
            this.position = position;
            this.operatorID = operatorID;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return new GraphOperatorCoordinator(context, position);
        }
    }

}
