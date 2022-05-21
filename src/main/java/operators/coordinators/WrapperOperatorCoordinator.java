package operators.coordinators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class WrapperOperatorCoordinator implements OperatorCoordinator {

    private static final HashMap<Short, SubtaskGateway[]> subtaskGateways = new HashMap<>(); // Subtask gateway for all operators
    private final Context context;
    private final short position;


    public WrapperOperatorCoordinator(Context context, short position) {
        this.context = context;
        this.position = position;
        subtaskGateways.put(position, new SubtaskGateway[context.currentParallelism()]);
    }

    @Override
    public void start() {
    }

    @Override
    public void subtaskReady(int subtaskIndex, SubtaskGateway subtaskGateway) {
        subtaskGateways.get(position)[subtaskIndex] = subtaskGateway;
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {

    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {

    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {

    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void notifyCheckpointComplete(long l) {

    }

    @Override
    public void subtaskReset(int i, long l) {
    }

    /**
     * The factory of {@link WrapperOperatorCoordinator}.
     */
    public static class HeadOperatorCoordinatorProvider implements Provider {

        private final OperatorID operatorId;
        private final short position;

        public HeadOperatorCoordinatorProvider(
                OperatorID operatorId, short position) {
            this.operatorId = operatorId;
            this.position = position;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new WrapperOperatorCoordinator(context, position);
        }
    }
}
