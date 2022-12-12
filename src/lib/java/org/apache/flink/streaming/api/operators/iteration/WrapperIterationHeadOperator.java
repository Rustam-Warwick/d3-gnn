package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.function.Consumer;

/**
 * HEAD logic wrapper around the main operator logic for {@link OneInputStreamOperator}
 * Currently only supports single input stream operator but can be extended to support many sources as well
 * @param <OUT> Output Type
 */
public class WrapperIterationHeadOperator<OUT> implements StreamOperator<OUT>, OneInputStreamOperator<Object, OUT>, OperatorEventHandler {

    /**
     * Unique ID for the Iteration
     */
    protected final int iterationID;

    /**
     * Mailbox Executor to attach to
     */
    protected final MailboxExecutor mailboxExecutor;

    /**
     * Termination Detection point reached can close this operator
     */
    protected boolean readyToFinish = false;

    /**
     * Full ID of {@link IterationChannel}
     */
    protected final IterationChannelKey channelID;

    /**
     * Gateway to operator coordinator
     */
    protected final OperatorEventGateway operatorEventGateway;

    /**
     * Main {@link StreamOperator} that is wrapped by this HEAD
     */
    protected final AbstractStreamOperator<OUT> bodyOperator;

    /**
     * Consumer of {@link OperatorEvent} depending on weather body implements {@link OperatorEventHandler} or not
     */
    protected final Consumer<OperatorEvent> operatorEventConsumer;

    /**
     * Just References with OneInput type to avoid constant type casting
     */
    protected final OneInputStreamOperator<Object, OUT> oneInputBodyOperatorRef;

    /**
     * Just Reference to OperatorEventHandler to avoid constant type casting
     */
    protected final OperatorEventHandler bodyOperatorEventHandleRef;


    public WrapperIterationHeadOperator(int iterationID, MailboxExecutor mailboxExecutor, AbstractStreamOperator<OUT> bodyOperator, StreamOperatorParameters<OUT> parameters) {
        this.iterationID = iterationID;
        this.mailboxExecutor = mailboxExecutor;
        this.bodyOperator = bodyOperator;
        this.channelID = new IterationChannelKey(parameters.getContainingTask().getEnvironment().getJobID(), iterationID, parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber(), parameters.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        parameters.getOperatorEventDispatcher().registerEventHandler(getOperatorID(), this);
        this.oneInputBodyOperatorRef = (bodyOperator instanceof OneInputStreamOperator)? (OneInputStreamOperator<Object, OUT>) bodyOperator :null;
        this.bodyOperatorEventHandleRef = (bodyOperator instanceof OperatorEventHandler)? (OperatorEventHandler) bodyOperator :null;
        operatorEventConsumer = bodyOperatorEventHandleRef == null?this::handleOperatorEventSelf:this::handleOperatorEventWithBody;
    }

    @Override
    public void open() throws Exception {
        bodyOperator.open();
    }

    @Override
    public void finish() throws Exception {
        bodyOperator.finish();
    }

    @Override
    public void close() throws Exception {
        bodyOperator.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        bodyOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
        return bodyOperator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    /**
     * Process Feedback messages if body is {@link OneInputStreamOperator}
     */
    public void processOneInputFeedback(StreamRecord<Object> el){
        try{
            setKeyContextElement(el);
            processElement(el);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
        bodyOperator.initializeState(streamTaskStateManager);
        IterationChannel<StreamRecord<Object>> channel = IterationChannelBroker.getBroker().getIterationChannel(channelID);
        bodyOperator.getContainingTask().getCancelables().registerCloseable(channel);
        channel.setConsumer(this::processOneInputFeedback, mailboxExecutor);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        bodyOperator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        bodyOperator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return bodyOperator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return bodyOperator.getOperatorID();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        bodyOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        bodyOperator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public Object getCurrentKey() {
        return bodyOperator.getCurrentKey();
    }

    @Override
    public void setCurrentKey(Object key) {
        bodyOperator.setCurrentKey(key);
    }

    @Override
    public void processElement(StreamRecord<Object> element) throws Exception{
        oneInputBodyOperatorRef.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if(mark.getTimestamp() == Long.MAX_VALUE && !readyToFinish){
            // Enter the termination loop
            if(bodyOperator.getRuntimeContext().getIndexOfThisSubtask() == 0) operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.StartTermination());
            while(!readyToFinish){
                mailboxExecutor.yield();
            }
        }
        oneInputBodyOperatorRef.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        oneInputBodyOperatorRef.processWatermarkStatus(watermarkStatus);
    }

    /**
     * Handle operator event if body is NOT {@link OperatorEventHandler}
     */
    public void handleOperatorEventSelf(OperatorEvent evt){
        if(evt instanceof WrapperIterationHeadOperatorCoordinator.RequestScan){
            // Requested scan for termination detection
            operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.ResponseScan(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter().getCount()));
        }else if(evt instanceof WrapperIterationHeadOperatorCoordinator.Terminate){
            // Ready to Terminate
            readyToFinish = true;
        }
    }

    /**
     * Handle operator event if body IS {@link OperatorEventHandler}
     */
    public void handleOperatorEventWithBody(OperatorEvent evt){
        bodyOperatorEventHandleRef.handleOperatorEvent(evt);
        handleOperatorEventSelf(evt);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        operatorEventConsumer.accept(evt);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        oneInputBodyOperatorRef.processLatencyMarker(latencyMarker);
    }

}
