package org.apache.flink.streaming.api.operators.graph;

import elements.GraphEvent;
import elements.GraphOp;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * SubCoordinator for handling the start and stop of the training loop
 */
public class TrainingSubCoordinator extends GraphOperatorCoordinator.GraphOperatorSubCoordinator {

    protected transient RequestTrainingEventsHandler requestTrainingEventsHandler;

    protected transient RequestMiniBatchCountEventHandler requestMiniBatchCountEventHandler;

    public TrainingSubCoordinator(GraphOperatorCoordinator mainCoordinator, float percentOfRequestToStart, int miniBatchSize){
        super(mainCoordinator);
        Preconditions.checkState(percentOfRequestToStart > 0 && percentOfRequestToStart <= 1, "Percent should be between (0 and 1]");
        Preconditions.checkState(miniBatchSize > 0, "Mini-batch should be more than 0");
        requestTrainingEventsHandler = new RequestTrainingEventsHandler(percentOfRequestToStart);
        requestMiniBatchCountEventHandler = new RequestMiniBatchCountEventHandler(miniBatchSize);
    }

    public TrainingSubCoordinator(GraphOperatorCoordinator mainCoordinator) {
        this(mainCoordinator, 0.3f, 4096);
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if((event instanceof RequestTraining)) requestTrainingEventsHandler.accept((RequestTraining) event);
        else if((event instanceof RequestMiniBatch)) requestMiniBatchCountEventHandler.accept((RequestMiniBatch) event);
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


     /**
     * Handler for {@link RequestTraining} events
     */
    public class RequestTrainingEventsHandler implements Consumer<RequestTraining> {

        private final short numRequestEventsToStart;

        private short numReceivedRequestEvents;

        public RequestTrainingEventsHandler(float percentOfRequestToStart) {
            this.numRequestEventsToStart = (short) (mainCoordinator.context.currentParallelism() * percentOfRequestToStart);
        }

        public void accept(RequestTraining ignored){
            if(++numReceivedRequestEvents == numRequestEventsToStart) {
                for (SubtaskGateway subTaskGateway : mainCoordinator.positionToCoordinators.get((short) 0).subTaskGateways) {
                    subTaskGateway.sendEvent(new FlushForTraining());
                }
                numReceivedRequestEvents = 0;
            }
        }
    }

    /**
     * Handler for detecting mini-batch count based on {@link RequestMiniBatch} event
     */
    public class RequestMiniBatchCountEventHandler implements Consumer<RequestMiniBatch>{
        protected final int miniBatchSize;

        protected final IntArrayList dataSizeFromOperators;

        public RequestMiniBatchCountEventHandler(int miniBatchSize) {
            this.miniBatchSize = miniBatchSize;
            this.dataSizeFromOperators = new IntArrayList(0);
        }

        public void accept(RequestMiniBatch requestMiniBatch){
            dataSizeFromOperators.add(requestMiniBatch.trainingDataSize);
            if(dataSizeFromOperators.size() == mainCoordinator.context.currentParallelism()){
                short miniBatchCount = (short) Math.ceil((double) dataSizeFromOperators.intStream().sum() / miniBatchSize);
                for (SubtaskGateway subTaskGateway : mainCoordinator.subTaskGateways) {
                    subTaskGateway.sendEvent(new StartTrainingWithMiniBatch(miniBatchCount));
                }
                dataSizeFromOperators.clear();
            }
        }

    }

    /**
     * Final layer requests this coordinator to start the training loop
     * Actual training loop is started once {@code numRequestEventsToStart} of such events are received
     */
    public static class RequestTraining implements OperatorEvent{}


    /**
     * Request the number of mini-batches for training
     */
    public static class RequestMiniBatch implements OperatorEvent{
        protected int trainingDataSize;

        public RequestMiniBatch(int trainingDataSize) {
            this.trainingDataSize = trainingDataSize;
        }
    }

    /**
     * Response with the number of mini batches for this training iteration
     */
    public static class StartTrainingWithMiniBatch implements OperatorEvent{
        public short miniBatches;

        public short epochs;

        public StartTrainingWithMiniBatch(short miniBatches, short epochs) {
            this.miniBatches = miniBatches;
            this.epochs = epochs;
        }

        public StartTrainingWithMiniBatch(short miniBatches) {
            this(miniBatches, (short) 1);
        }
    }

    /**
     * Event that is sent to the {@link DatasetSplitterOperator} indicating that training is entering to flush phase
     * This event will further go down the pipeline until the last operator is met
     * Event is doing double iteration in every layer except the last one
     */
    public static class FlushForTraining extends GraphEvent{

        protected transient byte iteration;

        protected transient short numReceived;

        protected transient short shouldReceive;

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {
            if(incoming == null){
                // First ever occurrence
                shouldReceive = (short) Arrays.stream(pool.graphRuntimeContext.getInputGates()).mapToInt(InputGate::getNumberOfInputChannels).sum();
            }
            if(++numReceived == shouldReceive && checkEviction(pool)) {
                pool.evict(this); // Evict so plugins can respond
                pushToNext(pool);
            }
        }

        public void pushToNext(GraphEventPool pool){
            if(!pool.graphRuntimeContext.isLast()) pool.graphRuntimeContext.broadcast(new GraphOp(this));
        }

        public boolean checkEviction(GraphEventPool pool){
            if(pool.graphRuntimeContext.isLast()) return true;
            if(iteration == 2) return true;
            iteration++;
            numReceived = 0;
            shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks();
            pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);
            return false;
        }

    }



    /**
     * Phaser for the backward messages during the training
     * Doing double iteration and backward until the first layer is received
     * Starting ForwardPhaser in the first layer
     */
    public static class BackwardPhaser extends GraphEvent {

        public transient byte iteration;

        transient short numReceived;

        transient short shouldReceive;


        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {
            if(incoming == null){
                shouldReceive = (short) pool.graphRuntimeContext.getNumOfOutChannels();
            }
            if(++numReceived == shouldReceive){
                if(iteration++ == 0) {
                    pool.eventHandler.handleOperatorEvent(this);
                    numReceived = 0;
                    shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks();
                    pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);

                }else{
                    pool.evict(this);
                    if(!pool.graphRuntimeContext.isFirst()) pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.BACKWARD_OUTPUT_TAG);
                    else startForward(pool);
                }
            }
        }

        public void startForward(GraphEventPool pool){

        }
    }

    /**
     * Forward Phaser that does tripe iteration in mid-layers and single iteration in the last one
     * Created by the BackwardPhaser in the last layer
     */
    public static class ForwardPhaser extends GraphEvent{

        public transient byte iteration;

        transient short numReceived;

        transient short shouldReceive;

        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {

        }

        public void pushToNext(GraphEventPool pool){
            if(!pool.graphRuntimeContext.isLast()) pool.graphRuntimeContext.broadcast(new GraphOp(this));
        }

        public boolean checkEviction(GraphEventPool pool){
            if(pool.graphRuntimeContext.isLast()) return true;
            if(iteration == 2) return true;
            iteration++;
            numReceived = 0;
            shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks();
            pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);
            return false;
        }
    }

    public static class ResumeDataFlow extends GraphEvent{
        @Override
        public void merge(GraphEventPool pool, @org.jetbrains.annotations.Nullable GraphEvent incoming) {

        }
    }

}
