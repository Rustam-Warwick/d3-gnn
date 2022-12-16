package operators.coordinators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Operator coordinator for all the wrapper operators
 * In other words this operator coordinator actually manages a chain of operators in the GNN pipeline
 * Communication is done through the singleton subtaskGateways.
 * <p>
 * This is the primary operator that keeps track of the length of the pipeline and delegate particular
 * {@link OperatorEvent} to delegated {@link WrapperOperatorEventHandler}
 * </p>
 *
 * @todo can we have more than one such chains in one session? This only supports one. Good to also have a unique id for one stream of ML pipeline
 */
public class WrapperOperatorCoordinator implements OperatorCoordinator {

    protected static final Map<Short, SubtaskGateway[]> subtaskGateways = new ConcurrentHashMap<>(); // Subtask gateway for all operators in the chain

    protected final Context context;

    protected final short position;

    protected final short layers;

    private final HashMap<Class<? extends OperatorEvent>, List<WrapperOperatorEventHandler>> class2Handler = new HashMap<>(10);

    private final HashSet<WrapperOperatorEventHandler> handlers = new HashSet<>(3);

    public WrapperOperatorCoordinator(Context context, short position, short layers) {
        this.context = context;
        this.position = position;
        this.layers = layers;
        subtaskGateways.put(position, new SubtaskGateway[context.currentParallelism()]);
    }

    @Override
    public void start() throws Exception {
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.start();
        }
    }

    @Override
    public void close() throws Exception {
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.close();
        }

    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        resultFuture.complete(new byte[0]);
        // To be implemented
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        System.out.println("notifyCheckpointComplete");
        // To be implemented
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        System.out.println("resetToCheckpoint");
        // To be implemented
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        List<WrapperOperatorEventHandler> handlers = class2Handler.getOrDefault(event.getClass(), Collections.emptyList());
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.handleEventFromOperator(subtask, attemptNumber, event);
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.executionAttemptFailed(subtask, attemptNumber, reason);
        }
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.executionAttemptReady(subtask, attemptNumber, gateway);
        }
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.subtaskReset(subtask, checkpointId);
        }
    }


    /**
     * Subscribe an event handler with this coordinator
     *
     * @param e Event Handler
     */
    public void subscribe(WrapperOperatorEventHandler e) {
        for (Class<? extends OperatorEvent> eventClass : e.getEventClasses()) {
            class2Handler.compute(eventClass, (key, val) -> {
                if (val == null) {
                    return new ArrayList<>(List.of(e));
                }
                val.add(e);
                return val;
            });
        }
        handlers.add(e);
        e.setCoordinator(this);
    }

    /**
     * The factory of {@link WrapperOperatorCoordinator}.
     */
    public static class WrappedOperatorCoordinatorProvider implements Provider {

        private final OperatorID operatorId;

        private final short position;

        private final short layers;

        public WrappedOperatorCoordinatorProvider(
                OperatorID operatorId, short position, short layers) {
            this.operatorId = operatorId;
            this.position = position;
            this.layers = layers;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            WrapperOperatorCoordinator tmp = new WrapperOperatorCoordinator(context, position, layers);
            return tmp;
        }
    }
}
