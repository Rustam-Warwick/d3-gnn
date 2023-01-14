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
     * <p>
      *     Handler for {@link RequestTraining} type events
      *     Once {@code percentOfRequestsToStart * parallelism} operators have sent this event will trigger start training
      *     Triggering start training means sending {@link FlushForTraining} to the Splitter operator
     * </p>
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

        public void accept(RequestMiniBatch requestMiniBatch){
            dataSizeFromOperators.add(requestMiniBatch.trainingDataSize);
            if(dataSizeFromOperators.size() == mainCoordinator.context.currentParallelism()){
                short miniBatchCount = (short) Math.ceil((double) dataSizeFromOperators.intStream().sum() / miniBatchSize);
                for (SubtaskGateway subTaskGateway : mainCoordinator.subTaskGateways) {
                    subTaskGateway.sendEvent(new StartTraining(miniBatchCount));
                }
                dataSizeFromOperators.clear();
            }
        }

    }

    /**
     *  <p>
     *      Event sent from the last layer scheduler requesting to start training
     *      Event handled by the {@link RequestTrainingEventsHandler} which generates {@link FlushForTraining} events
     *      By default when a certain percent of operators have sent this event the training phase will begin
     *  </p>
     */
    public static class RequestTraining implements OperatorEvent{}


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
            this(miniBatches, (short) 4);
        }
    }

    /**
     * <p>
     *      Resume the inference mode of the {@link DatasetSplitterOperator}
     *      To be sent once entire batch and epochs have been trained & processed
     *      This event is sent from last operator to this guy
     *      And also from this to SPLITTER
     * </p>
     */
    public static class ResumeInference extends GraphEvent {

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
     * Event that is sent to the {@link DatasetSplitterOperator} indicating that training is entering to training phase
     * {@link DatasetSplitterOperator} will then fill all output channels with this messages which is in turn going to flush the pipeline
     * Last layer seeing this event should trigger {@link RequestMiniBatch} to gather mini-batch and epoch count for training
     * Intermediate layers -> iterate, iterate, evict and send forward
     * Last layer -> evict
     * </p>
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
            if(++numReceived == shouldReceive) {
                if(pool.graphRuntimeContext.isLast() || iteration == 3 ){
                    // Evict first then send
                    pool.evict(this);
                    if(!pool.graphRuntimeContext.isLast()) pool.graphRuntimeContext.broadcast(new GraphOp(this));

                }else{
                    iteration++;
                    numReceived = 0;
                    shouldReceive = (short) pool.graphRuntimeContext.getNumberOfParallelSubtasks();
                    pool.graphRuntimeContext.broadcast(new GraphOp(this), OutputTags.ITERATE_OUTPUT_TAG);
                }

            }
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
