package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator for {@link WrapperIterationHeadOperator}
 * Similarly it optionally wraps around body operator's {@link OperatorCoordinator} but on top of it implements <strong>distributed termination detection</strong>
 */
public class WrapperIterationHeadOperatorCoordinator implements OperatorCoordinator {

    /**
     * Coordinator for the internal operator. All the events are also directed to this body operator coordinator if it exists
     */
    @Nullable
    protected final OperatorCoordinator bodyOperatorCoordinator;

    /**
     * Operator Context
     */
    protected final Context context;

    /**
     * Hold the helper class
     */
    protected final TerminationDetectionController controller;

    /**
     * Gateways to subtasks
     */
    protected final SubtaskGateway[] gateways;

    public WrapperIterationHeadOperatorCoordinator(@Nullable OperatorCoordinator bodyOperatorCoordinator, Context context) {
        this.bodyOperatorCoordinator = bodyOperatorCoordinator;
        this.context = context;
        this.gateways = new SubtaskGateway[context.currentParallelism()];
        this.controller = (TerminationDetectionController) context.getCoordinatorStore().compute("termination_detection_controller", (key, val) -> {
            if (val == null) val = new TerminationDetectionController();
            return val;
        });
    }

    @Override
    public void start() throws Exception {
        controller.addCoordinator(this);
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.start();
    }

    @Override
    public void close() throws Exception {
        controller.removeCoordinator(this);
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if (event instanceof StartTermination) controller.startTermination();
        if (event instanceof ResponseScan) controller.consumeResponse(((ResponseScan) event).terminateReady);
        if (bodyOperatorCoordinator != null)
            bodyOperatorCoordinator.handleEventFromOperator(subtask, attemptNumber, event);

    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.checkpointCoordinator(checkpointId, resultFuture);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        if (bodyOperatorCoordinator != null) bodyOperatorCoordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        gateways[subtask] = null;
        controller.removeSubOperator();
        if (bodyOperatorCoordinator != null)
            bodyOperatorCoordinator.executionAttemptFailed(subtask, attemptNumber, reason);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        gateways[subtask] = gateway;
        controller.addSubOperator();
        if (bodyOperatorCoordinator != null)
            bodyOperatorCoordinator.executionAttemptReady(subtask, attemptNumber, gateway);
    }

    /**
     * Start a scan for {@link WrapperIterationHeadOperator}
     */
    public void doScan() {
        for (SubtaskGateway gateway : gateways) {
            if (gateway != null) {
                gateway.sendEvent(new RequestScan());
            }
        }
    }

    /**
     * Terminate the {@link WrapperIterationHeadOperator}
     */
    public void doTerminate() {
        for (SubtaskGateway gateway : gateways) {
            if (gateway != null) {
                gateway.sendEvent(new Terminate());
            }
        }
    }

    /**
     * Helper class instance of which is shared amongst {@link WrapperIterationHeadOperatorCoordinator} in one job
     * Counts the number of messages from each sub-operator and detects when to terminate the operator
     */
    private static class TerminationDetectionController extends Thread {

        /**
         * List of all Coordinators
         */
        protected final List<WrapperIterationHeadOperatorCoordinator> coordinators = new ArrayList<>(4);

        /**
         * Number of sub-operators with iteration HEAD logic
         */
        protected final AtomicInteger numIterationSubOperators = new AtomicInteger(0);

        /**
         * Number of messages received from HEAD sub-operators
         */
        protected final AtomicInteger receivedFromIterationOperators = new AtomicInteger(0);

        /**
         * Found termination point
         */
        protected final AtomicBoolean terminationFound = new AtomicBoolean(false);

        /**
         * Running this Thread
         */
        protected final AtomicBoolean startedOnce = new AtomicBoolean(false);

        /**
         * Add newly created {@link WrapperIterationHeadOperatorCoordinator} object to the list
         */
        synchronized void addCoordinator(WrapperIterationHeadOperatorCoordinator coordinator) {
            coordinators.add(coordinator);
        }

        /**
         * Remove coordinator. If the coordinator is closed
         */
        synchronized void removeCoordinator(WrapperIterationHeadOperatorCoordinator coordinator) {
            coordinators.remove(coordinator);
            if (coordinators.isEmpty() && isAlive()) interrupt();
        }

        /**
         * New Sub-Operator added increment counter
         */
        void addSubOperator() {
            numIterationSubOperators.incrementAndGet();
        }

        /**
         * Sub-Operator failed increment counter
         */
        void removeSubOperator() {
            numIterationSubOperators.decrementAndGet();
        }

        /**
         * One head has reached finish block startFlushing the distributed termination detection
         */
        void startTermination() {
            if (!startedOnce.getAndSet(true)) start();
        }

        /**
         * Consume Scan response
         */
        void consumeResponse(boolean response) {
            terminationFound.compareAndExchange(true, response);
            if(receivedFromIterationOperators.incrementAndGet() == numIterationSubOperators.get()) {
                synchronized (this) {
                    notify();
                }
            };
        }

        @Override
        public void run() {
            try {
                while (!terminationFound.get()) {
                    terminationFound.set(true); // Assume found if not negated by sub-operator
                    receivedFromIterationOperators.set(0);
                    coordinators.forEach(WrapperIterationHeadOperatorCoordinator::doScan);
                    synchronized (this) {
                        while (receivedFromIterationOperators.get() < numIterationSubOperators.get()) {
                            wait();
                        }
                    }
                    if (terminationFound.get()) {
                        coordinators.forEach(WrapperIterationHeadOperatorCoordinator::doTerminate);
                    } else {
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Message for Starting Termination Detection
     * Should be sent from {@link WrapperIterationHeadOperator} to {@link WrapperIterationHeadOperatorCoordinator}
     */
    public static class StartTermination implements OperatorEvent {
    }

    /**
     * Message for Requesting Scanning of HEAD operator
     * Should be sent from {@link WrapperIterationHeadOperatorCoordinator} to {@link WrapperIterationHeadOperator}
     */
    public static class RequestScan implements OperatorEvent {
    }

    /**
     * Scan Response Result from operators
     * Should be sent from {@link WrapperIterationHeadOperator} to {@link WrapperIterationHeadOperatorCoordinator}
     */
    public static class ResponseScan implements OperatorEvent {
        public final boolean terminateReady;

        public ResponseScan(boolean terminateReady) {
            this.terminateReady = terminateReady;
        }
    }

    /**
     * Terminate the operator
     * Should be sent from {@link WrapperIterationHeadOperatorCoordinator} to {@link WrapperIterationHeadOperator}
     */
    public static class Terminate implements OperatorEvent {
    }

    /**
     * Simple Provider implementation
     */
    public static class WrapperIterationHeadOperatorCoordinatorProvider implements OperatorCoordinator.Provider {

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
            return new WrapperIterationHeadOperatorCoordinator(bodyOperatorCoordinatorProvider == null ? null : bodyOperatorCoordinatorProvider.create(context), context);
        }
    }
}
