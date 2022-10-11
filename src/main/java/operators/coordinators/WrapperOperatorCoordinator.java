package operators.coordinators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final HashMap<Class<? extends OperatorEvent>, List<WrapperOperatorEventHandler>> delegateHandlers;


    public WrapperOperatorCoordinator(Context context, short position, short layers) {
        this.context = context;
        this.position = position;
        this.layers = layers;
        this.delegateHandlers = new HashMap<>(10);
        subtaskGateways.put(position, new SubtaskGateway[context.currentParallelism()]);
    }

    @Override
    public void start() throws Exception {
        for (List<WrapperOperatorEventHandler> value : delegateHandlers.values()) {
            for (WrapperOperatorEventHandler wrapperOperatorEventHandler : value) {
                wrapperOperatorEventHandler.start();
            }
        }
    }

    @Override
    public void close() throws Exception {
        for (List<WrapperOperatorEventHandler> value : delegateHandlers.values()) {
            for (WrapperOperatorEventHandler wrapperOperatorEventHandler : value) {
                wrapperOperatorEventHandler.close();
            }
        }

    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        List<WrapperOperatorEventHandler> handlers = delegateHandlers.getOrDefault(event.getClass(), null);
        assert handlers != null;
        for (WrapperOperatorEventHandler handler : handlers) {
            handler.handleEventFromOperator(subtask, event);
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
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        for (List<WrapperOperatorEventHandler> value : delegateHandlers.values()) {
            for (WrapperOperatorEventHandler wrapperOperatorEventHandler : value) {
                wrapperOperatorEventHandler.subtaskFailed(subtask, reason);
            }
        }
        subtaskGateways.get(position)[subtask] = null; // make null
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        for (List<WrapperOperatorEventHandler> value : delegateHandlers.values()) {
            for (WrapperOperatorEventHandler wrapperOperatorEventHandler : value) {
                wrapperOperatorEventHandler.subtaskReset(subtask, checkpointId);
            }
        }
    }

    @Override
    public void subtaskReady(int subtaskIndex, SubtaskGateway subtaskGateway) {
        subtaskGateways.get(position)[subtaskIndex] = subtaskGateway;
        for (List<WrapperOperatorEventHandler> value : delegateHandlers.values()) {
            for (WrapperOperatorEventHandler wrapperOperatorEventHandler : value) {
                wrapperOperatorEventHandler.subtaskReady(subtaskIndex, subtaskGateway);
            }
        }
    }

    /**
     * Subscribe an event handler with this coordinator
     *
     * @param e Event Handler
     */
    public void subscribe(WrapperOperatorEventHandler e) {
        for (Class<? extends OperatorEvent> eventClass : e.getEventClasses()) {
            delegateHandlers.compute(eventClass, (key, val) -> {
                if (val == null) {
                    return new ArrayList<>(List.of(e));
                }
                val.add(e);
                return val;
            });
        }
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
            tmp.subscribe(new BatchedTrainingEventHandler());
            return tmp;
        }
    }
}
