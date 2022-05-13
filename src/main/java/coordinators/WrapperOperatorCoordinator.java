package coordinators;

import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public class WrapperOperatorCoordinator implements OperatorCoordinator {
    protected final Context context;
    protected final IterationID iterationID;
    protected final short position;
    protected final short totalLayers;

    public WrapperOperatorCoordinator(Context context, IterationID iterationID, short position, short totalLayers) {
        this.context = context;
        this.iterationID = iterationID;
        this.position = position;
        this.totalLayers = totalLayers;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
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
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
    }


    public static class WrapperOperatorCoordinatorProvider implements Provider {

        private final OperatorID operatorId;

        private final IterationID iterationId;

        private final short position;

        private final short totalLayers;

        public WrapperOperatorCoordinatorProvider(
                OperatorID operatorId, IterationID iterationId, short position, short totalLayers) {
            this.operatorId = operatorId;
            this.iterationId = iterationId;
            this.position = position;
            this.totalLayers = totalLayers;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new WrapperOperatorCoordinator(context, iterationId, position, totalLayers);
        }
    }

}
