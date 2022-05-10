package coordinators;

import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

public class WrapperOperatorCoordinator implements OperatorCoordinator {
    protected final Context context;
    public WrapperOperatorCoordinator(Context context){
        this.context = context;
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

        private final int totalHeadParallelism;

        public WrapperOperatorCoordinatorProvider(
                OperatorID operatorId, IterationID iterationId, int totalHeadParallelism) {
            this.operatorId = operatorId;
            this.iterationId = iterationId;
            this.totalHeadParallelism = totalHeadParallelism;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
//            SharedProgressAligner sharedProgressAligner =
//                    SharedProgressAligner.getOrCreate(
//                            iterationId,
//                            totalHeadParallelism,
//                            context,
//                            () ->
//                                    Executors.newSingleThreadScheduledExecutor(
//                                            runnable -> {
//                                                Thread thread = new Thread(runnable);
//                                                thread.setName(
//                                                        "SharedProgressAligner-" + iterationId);
//                                                return thread;
//                                            }));
            return new WrapperOperatorCoordinator(context);
        }
    }

}
