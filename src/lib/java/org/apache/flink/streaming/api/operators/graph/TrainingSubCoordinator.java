package org.apache.flink.streaming.api.operators.graph;

import elements.GraphEvent;
import elements.GraphOp;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.iteration.WrapperIterationHeadOperator;
import org.apache.flink.streaming.api.operators.iteration.WrapperIterationHeadOperatorCoordinator;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * SubCoordinator for handling the start and stop of the training loop
 */
public class TrainingSubCoordinator extends GraphOperatorCoordinator.GraphOperatorSubCoordinator {

    protected final RequestTrainingEventsHandler requestTrainingEventsHandler;

    protected final RequestMiniBatchCountEventHandler requestMiniBatchCountEventHandler;

    protected final PipelineFlusherController pipelineFlusherController;

    public TrainingSubCoordinator(GraphOperatorCoordinator baseCoordinator, float percentOfRequestToStart, int miniBatchSize){
        super(baseCoordinator);
        Preconditions.checkState(percentOfRequestToStart > 0 && percentOfRequestToStart <= 1, "Percent should be between (0 and 1]");
        Preconditions.checkState(miniBatchSize > 0, "Mini-batch should be more than 0");
        requestTrainingEventsHandler = new RequestTrainingEventsHandler(percentOfRequestToStart);
        requestMiniBatchCountEventHandler = new RequestMiniBatchCountEventHandler(miniBatchSize);
        pipelineFlusherController = (PipelineFlusherController) baseCoordinator.context.getCoordinatorStore().compute("pipeline_flusher_controller", (key, val) -> {
           if(val != null) return val;
           return new PipelineFlusherController();
        });
    }

    public TrainingSubCoordinator(GraphOperatorCoordinator mainCoordinator) {
        this(mainCoordinator, 1f, 4096);
    }

    @Override
    public void start() throws Exception {
        pipelineFlusherController.addCoordinator(this);
    }

    @Override
    public void close() throws Exception {
        pipelineFlusherController.removeCoordinator(this);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if(event instanceof RequestTraining) requestTrainingEventsHandler.accept((RequestTraining) event);
        else if(event instanceof ResponseScan) pipelineFlusherController.consumeResponse(((ResponseScan) event).terminateReady);
//        else if((event instanceof RequestMiniBatch)) requestMiniBatchCountEventHandler.accept((RequestMiniBatch) event);
//        else if(event instanceof ResumeInference) {
//            requestMiniBatchCountEventHandler.clear();
//            requestTrainingEventsHandler.clear();
//        }
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
        pipelineFlusherController.removeSubOperator();
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        pipelineFlusherController.addSubOperator();
    }

    /**
     * Start a scan for {@link WrapperIterationHeadOperator}
     */
    public void doScan() {
        for (SubtaskGateway subTaskGateway : baseCoordinator.subTaskGateways) {
            if (subTaskGateway != null) subTaskGateway.sendEvent(new RequestScan());
        }
    }

    /**
     * Start the training
     */
    public void startTraining(){
        for (SubtaskGateway subTaskGateway : baseCoordinator.subTaskGateways) {
            if(subTaskGateway != null) subTaskGateway.sendEvent(new IngressStopped());
        }
    }

    /**
     * <p>
      *     Handler for {@link RequestTraining} type events
      *     Once {@code percentOfRequestsToStart * parallelism} operators have sent this event will trigger start training
      *     Triggering start training means sending {@link StopIngress} to the Splitter operator and entering termination detection mode
     * </p>
     */
    public class RequestTrainingEventsHandler implements Consumer<RequestTraining> {

        private final short numRequestEventsToStart;

        private short numReceivedRequestEvents;

        public RequestTrainingEventsHandler(float percentOfRequestToStart) {
            this.numRequestEventsToStart = (short) (baseCoordinator.context.currentParallelism() * percentOfRequestToStart);
        }

        public void clear(){
            numReceivedRequestEvents = 0;
        }

        public void accept(RequestTraining ignored){
            if(++numReceivedRequestEvents == numRequestEventsToStart) {
                // Clear only after completing training
                for (SubtaskGateway subTaskGateway : baseCoordinator.positionToCoordinators.get((short) 0).subTaskGateways) {
                    subTaskGateway.sendEvent(new StopIngress());
                }
                synchronized (pipelineFlusherController){
                    pipelineFlusherController.notify();
                }
            }
        }
    }

    /**
     * <p>
     *     Handler encapsulating the reception of {@link RequestMiniBatch} events
     *     Sums the number of data items in each operator and then sends batch the number of mini-batches during the training
     * </p>
     */
    public class RequestMiniBatchCountEventHandler implements Consumer<RequestMiniBatch>{

        protected final int miniBatchSize;

        protected final IntArrayList dataSizeFromOperators;

        public RequestMiniBatchCountEventHandler(int miniBatchSize) {
            this.miniBatchSize = miniBatchSize;
            this.dataSizeFromOperators = new IntArrayList(0);
        }

        public void clear(){
            dataSizeFromOperators.clear();
        }

        public void accept(RequestMiniBatch requestMiniBatch){
            dataSizeFromOperators.add(requestMiniBatch.trainingDataSize);
            if(dataSizeFromOperators.size() == baseCoordinator.context.currentParallelism()){
                // Clear after training is done
                short miniBatchCount = (short) Math.ceil((double) dataSizeFromOperators.intStream().sum() / miniBatchSize);
                for (SubtaskGateway subTaskGateway : baseCoordinator.subTaskGateways) {
                    subTaskGateway.sendEvent(new StartTraining(miniBatchCount));
                }
            }
        }

    }

    /**
     * Helper class instance of which is shared amongst {@link TrainingSubCoordinator} in one job
     * Counts the number of messages from each sub-operator and detects when to terminate the operator
     */
    private static class PipelineFlusherController extends Thread {

        /**
         * List of all Coordinators
         */
        protected final List<TrainingSubCoordinator> coordinators = new ArrayList<>(4);
        /**
         * Number of sub-operators with iteration HEAD logic
         */
        protected final AtomicInteger numSubOperators = new AtomicInteger(0);
        /**
         * Number of messages received from HEAD sub-operators
         */
        protected final AtomicInteger receivedFromSubOperators = new AtomicInteger(0);
        /**
         * Found termination point
         */
        protected final AtomicBoolean flushed = new AtomicBoolean(false);

        public PipelineFlusherController(){start();}

        /**
         * Add newly created {@link WrapperIterationHeadOperatorCoordinator} object to the list
         */
        synchronized void addCoordinator(TrainingSubCoordinator coordinator) {
            coordinators.add(coordinator);
        }

        /**
         * Remove coordinator. If the coordinator is closed
         */
        synchronized void removeCoordinator(TrainingSubCoordinator coordinator) {
            coordinators.remove(coordinator);
            if (coordinators.isEmpty() && isAlive()) interrupt();
        }

        /**
         * New Sub-Operator added increment counter
         */
        void addSubOperator() {
            numSubOperators.incrementAndGet();
        }

        /**
         * Sub-Operator failed increment counter
         */
        void removeSubOperator() {
            numSubOperators.decrementAndGet();
        }

        /**
         * Consume Scan response
         */
        void consumeResponse(boolean response) {
            flushed.compareAndExchange(true, response);
            if(receivedFromSubOperators.incrementAndGet() == numSubOperators.get()) {
                synchronized (this) {
                    notify();
                }
            };
        }

        @Override
        public void run() {
            try {
                while(true){
                    synchronized (this){
                        wait();
                    }
                    flushed.set(false);
                    while(!flushed.get()){
                        flushed.set(true); // Assume true unless negated
                        receivedFromSubOperators.set(0);
                        coordinators.forEach(TrainingSubCoordinator::doScan);
                        synchronized (this){
                            while(receivedFromSubOperators.get() < numSubOperators.get()){
                                wait();
                            }
                        }
                        if(flushed.get()){
                            coordinators.forEach(TrainingSubCoordinator::startTraining);
                        }else{
                            Thread.sleep(1000);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  <p>
     *      Event sent from some layer scheduler requesting to start training
     *      Event handled by the {@link RequestTrainingEventsHandler} which generates {@link StopIngress} events
     *      By default when a certain percent of operators have sent this event the training phase will begin
     *  </p>
     */
    public static class RequestTraining implements OperatorEvent{}

    /**
     * Scan to get termination for flushing
     */
    public static class RequestScan implements OperatorEvent{}

    /**
     * Scan Response Result from operators
     */
    public static class ResponseScan implements OperatorEvent {
        public final boolean terminateReady;

        public ResponseScan(boolean terminateReady) {
            this.terminateReady = terminateReady;
        }
    }

    /**
     * Event that is sent to the {@link DatasetSplitterOperator} indicating that training is entering to training phase and ingress is stopped
     */
    public static class StopIngress implements OperatorEvent{}

    /**
     * Marking the stoppage of the ingress
     */
    public static class IngressStopped implements OperatorEvent{}

     /**
     * <p>
     *      Resume the inference mode of the {@link DatasetSplitterOperator}
     *      To be sent once entire batch and epochs have been trained & processed
     *      This event is sent from last operator to this guy
     *      And also from this to SPLITTER
     * </p>
     */
    public static class ResumeIngress extends GraphEvent {

        protected transient short shouldReceive;

        protected transient short numReceived;

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {
            if(incoming == null){
                shouldReceive = (short) pool.graphRuntimeContext.getNumOfOutChannels();
            }
            if(++numReceived == shouldReceive){
                pool.evict(this);
                if(pool.graphRuntimeContext.getPosition() > 0) pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.BACKWARD_OUTPUT_TAG);
            }
        }
    }

    /**
     * <p>
     *     Event sent from the last layer training plugin to gather mini-batch count and epoch count
     *     Responded with {@link StartTraining}
     * </p>
     */
    public static class RequestMiniBatch implements OperatorEvent{
        protected int trainingDataSize;

        public RequestMiniBatch(int trainingDataSize) {
            this.trainingDataSize = trainingDataSize;
        }
    }

    /**
     * <p>
     *     Response of the {@link RequestMiniBatchCountEventHandler} to {@link RequestMiniBatch} events
     *     Once the quantity of data items have been received send the number of minibatches and epochs to start the training
     *     Only sent to the last operator
     * </p>
     */
    public static class StartTraining implements OperatorEvent{

        public short miniBatches;

        public short epochs;

        public StartTraining(short miniBatches, short epochs) {
            this.miniBatches = miniBatches;
            this.epochs = epochs;
        }

        public StartTraining(short miniBatches) {
            this(miniBatches, (short) 10);
        }
    }

    /**
     * <p>
     *     Event that is generated by the last layer trainer plugin to phase the synchronous training
     *     Phaser will travel backward starting from the last trainer plugin until the first layer is met
     *     In the first layer it will automatically start {@link ForwardPhaser}
     *     Non-First layer -> Evict, iterate, evict, back
     *     First layer -> Evict, Iterate, Evict, ForwardPhaser
     * </p>
     */
    public static class BackwardPhaser extends GraphEvent {

        public transient boolean isSecondPhase;

        transient short numReceived;

        transient short shouldReceive;

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {
            if(incoming == null){
                shouldReceive = (short) pool.graphRuntimeContext.getNumOfOutChannels(); // Receiving initially form the forward layer
            }
            if(++numReceived == shouldReceive){
                if(!isSecondPhase) {
                    pool.eventHandler.handleOperatorEvent(this);
                    numReceived = 0;
                    shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks(); // Then iterate
                    isSecondPhase = true;
                    pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);
                }else{
                    pool.evict(this);
                    if(!pool.graphRuntimeContext.isFirst()) pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.BACKWARD_OUTPUT_TAG); // send back
                    else pool.addEvent(new ForwardPhaser()); // Start forward pass
                }
            }
        }
    }

    /**
     * <p>
     *     Event that is started from {@link BackwardPhaser} of the first operator
     *     Non-Last Layer -> Evict, Iterate, Evict, Iterate, Evict, Send forward
     *     Last-Layer -> Evict, Iterate, Evict
     * </p>
     */
    public static class ForwardPhaser extends GraphEvent{

        public transient byte iteration;

        transient short numReceived;

        transient short shouldReceive;

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {
            if(incoming == null){
                if(pool.graphRuntimeContext.isFirst()){
                    // Since it is locally created after backward pass need to make it immediately entering the if block
                    shouldReceive = 1;
                    numReceived = 0;
                }else{
                    shouldReceive = (short) pool.graphRuntimeContext.getNumOfOutChannels(OutputTags.BACKWARD_OUTPUT_TAG);
                }
            }
            if(++numReceived == shouldReceive){
                if(iteration < (pool.graphRuntimeContext.isLast()?2:3)){
                    pool.eventHandler.handleOperatorEvent(this);
                    numReceived = 0;
                    shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks();
                    iteration++;
                    pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);
                }else{
                    pool.evict(this);
                    if(!pool.graphRuntimeContext.isLast()) pool.graphRuntimeContext.broadcast(new GraphOp(this));
                }
            }
        }

    }

}
