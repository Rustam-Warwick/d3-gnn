package org.apache.flink.streaming.api.operators.graph;

import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Operator Coordinator for {@link GraphStorageOperator} and {@link DatasetSplitterOperator}
 * <p>
 * It only contains the linkage logic that allows one to access all coordinators across different positional operators
 * Actual computation logic is flexible and can be adjusted by passing {@link GraphOperatorSubCoordinator} and its providers
 * </p>
 */
public class GraphOperatorCoordinator implements OperatorCoordinator {

    /**
     * Context
     */
    protected final Context context;

    /**
     * Position of this coordinator operator in the pipeline
     */
    protected final short position;

    /**
     * Number of layers
     */
    protected final short layers;

    /**
     * Position -> Coordinator. Linking all the coordinators
     */
    protected final Short2ObjectOpenHashMap<GraphOperatorCoordinator> positionToCoordinators;

    /**
     * Sub coordinators
     */
    protected final GraphOperatorSubCoordinator[] graphOperatorSubCoordinators;

    /**
     * Gateways for this coordinator subtasks
     */
    protected final SubtaskGateway[] subTaskGateways;


    public GraphOperatorCoordinator(Context context, short position, short layers, GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
        this.context = context;
        this.position = position;
        this.layers = layers;
        this.subTaskGateways = new SubtaskGateway[context.currentParallelism()];
        this.positionToCoordinators = (Short2ObjectOpenHashMap<GraphOperatorCoordinator>) context.getCoordinatorStore().compute("graph_coordinators", (key, val) -> {
            if (val == null) val = new Short2ObjectOpenHashMap<>();
            ((Short2ObjectOpenHashMap<GraphOperatorCoordinator>) val).put(position, this);
            return val;
        });
        this.graphOperatorSubCoordinators = graphOperatorSubCoordinatorsProvider.apply(this);
    }

    @Override
    public void start() throws Exception {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.start();
        }
    }

    @Override
    public void close() throws Exception {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.close();
        }
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.handleEventFromOperator(subtask, attemptNumber, event);
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.checkpointCoordinator(checkpointId, resultFuture);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.resetToCheckpoint(checkpointId, checkpointData);
        }
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.subtaskReset(subtask, checkpointId);
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        subTaskGateways[subtask] = null;
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.executionAttemptFailed(subtask, attemptNumber, reason);
        }
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        subTaskGateways[subtask] = gateway;
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.executionAttemptReady(subtask, attemptNumber, gateway);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        OperatorCoordinator.super.notifyCheckpointAborted(checkpointId);
        for (GraphOperatorSubCoordinator graphOperatorSubCoordinator : graphOperatorSubCoordinators) {
            graphOperatorSubCoordinator.notifyCheckpointAborted(checkpointId);
        }
    }

    /**
     * Provider pattern for {@link GraphOperatorSubCoordinator}
     */
    public interface GraphOperatorSubCoordinatorsProvider extends Function<GraphOperatorCoordinator, GraphOperatorSubCoordinator[]>, Serializable {
    }

    /**
     * SubCoordinators for graph operators to embed custom logic behind
     */
    abstract public static class GraphOperatorSubCoordinator implements OperatorCoordinator {

        protected final GraphOperatorCoordinator baseCoordinator;

        public GraphOperatorSubCoordinator(GraphOperatorCoordinator baseCoordinator) {
            this.baseCoordinator = baseCoordinator;
        }
    }

    /**
     * Simple Sub-coordinator provider with empty values
     */
    public static class EmptyGraphOperatorSubCoordinatorsProvider implements GraphOperatorSubCoordinatorsProvider {
        @Override
        public GraphOperatorSubCoordinator[] apply(GraphOperatorCoordinator graphOperatorCoordinator) {
            return new GraphOperatorSubCoordinator[0];
        }
    }

    /**
     * Default one with {@link TrainingSubCoordinator}
     */
    public static class DefaultGraphOperatorSubCoordinatorsProvider implements GraphOperatorSubCoordinatorsProvider {
        @Override
        public GraphOperatorSubCoordinator[] apply(GraphOperatorCoordinator graphOperatorCoordinator) {
            return new GraphOperatorSubCoordinator[]{new TrainingSubCoordinator(graphOperatorCoordinator)};
        }
    }

    /**
     * Provider implementation for coordinator
     */
    public static class GraphOperatorCoordinatorProvider implements OperatorCoordinator.Provider {

        final protected short position;

        final protected short layers;

        final protected OperatorID operatorID;

        final protected GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider;

        public GraphOperatorCoordinatorProvider(short position, short layers, OperatorID operatorID, GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
            this.position = position;
            this.layers = layers;
            this.operatorID = operatorID;
            this.graphOperatorSubCoordinatorsProvider = graphOperatorSubCoordinatorsProvider;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return new GraphOperatorCoordinator(context, position, layers, graphOperatorSubCoordinatorsProvider);
        }
    }

}
