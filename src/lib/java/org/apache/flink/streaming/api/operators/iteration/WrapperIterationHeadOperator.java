package org.apache.flink.streaming.api.operators.iteration;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.LifeCycleControl;
import ai.djl.ndarray.NDManager;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.Counter;
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
 *
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
     * Full {@link IterationChannelKey} of {@link IterationChannel}
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
    protected final OperatorEventHandler operatorEventHandleBodyOperatorRef;
    /**
     * Just Reference to ExposingInternalTimerSerivce operator to avoid constant type casting
     */
    protected final ExposingInternalTimerService exposingInternalTimerServiceOperatorRef;
    /**
     * Counter of number of incoming messages to increments with iteration inputs
     */
    protected final Counter numRecordsInCounter;
    /**
     * Termination Detection point reached can close this operator
     */
    protected boolean readyToFinish = false;
    /**
     * If the elements in this channel are instances of {@link ai.djl.ndarray.LifeCycleControl}. If it is the case, need to resume on taking from buffer
     */
    protected Boolean isLifeCycle;
    /**
     * Link to {@link NDManager} to avoid constant access to {@link ThreadLocal}
     */
    protected NDManager manager = BaseNDManager.getManager();
    /**
     * Used for termination detection
     */
    protected long previousNumRecordsInValue;

    public WrapperIterationHeadOperator(int iterationID, MailboxExecutor mailboxExecutor, AbstractStreamOperator<OUT> bodyOperator, StreamOperatorParameters<OUT> parameters) {
        this.iterationID = iterationID;
        this.mailboxExecutor = mailboxExecutor;
        this.bodyOperator = bodyOperator;
        this.numRecordsInCounter = getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        this.channelID = new IterationChannelKey(parameters.getContainingTask().getEnvironment().getJobID(), iterationID, parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber(), parameters.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        parameters.getOperatorEventDispatcher().registerEventHandler(getOperatorID(), this);
        this.oneInputBodyOperatorRef = (bodyOperator instanceof OneInputStreamOperator) ? (OneInputStreamOperator<Object, OUT>) bodyOperator : null;
        this.operatorEventHandleBodyOperatorRef = (bodyOperator instanceof OperatorEventHandler) ? (OperatorEventHandler) bodyOperator : null;
        this.exposingInternalTimerServiceOperatorRef = (bodyOperator instanceof ExposingInternalTimerService) ? (ExposingInternalTimerService) bodyOperator : null;
        operatorEventConsumer = operatorEventHandleBodyOperatorRef == null ? this::handleOperatorEventSelf : this::handleOperatorEventWithBody;
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
    public void processOneInputFeedback(StreamRecord<Object> el) {
        try {
            if (isLifeCycle == null) isLifeCycle = el.getValue() instanceof LifeCycleControl;
            if (isLifeCycle) ((LifeCycleControl) el.getValue()).resume();
            numRecordsInCounter.inc();
            setKeyContextElement(el);
            processElement(el);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // NDManager is delayed per batch of iteration messages, and these messages can be quite large so it is good to delay them per element instead of per batch
            manager.resume();
            manager.delay();
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
    public void processElement(StreamRecord<Object> element) throws Exception {
        oneInputBodyOperatorRef.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() == Long.MAX_VALUE && !readyToFinish) {
            // Enter the termination loop
            if (bodyOperator.getRuntimeContext().getIndexOfThisSubtask() == 0)
                operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.StartTermination());
            while (!readyToFinish) {
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
    public void handleOperatorEventSelf(OperatorEvent evt) {
        if (evt instanceof WrapperIterationHeadOperatorCoordinator.RequestScan) {
            // Requested scan for termination detection
            final boolean[] hasTimers = new boolean[]{false};
            if (exposingInternalTimerServiceOperatorRef != null) {
                // If exposing check for timers to be finished
                try {
                    exposingInternalTimerServiceOperatorRef.getInternalTimerService().forEachProcessingTimeTimer((ns, timer) -> {
                        hasTimers[0] = true;
                        throw new Exception("Found, do not process rest");
                    });
                } catch (Exception ignored) {
                }
            }
            operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.ResponseScan(
                    !hasTimers[0] && numRecordsInCounter.getCount() == previousNumRecordsInValue));
            previousNumRecordsInValue = numRecordsInCounter.getCount();
        } else if (evt instanceof WrapperIterationHeadOperatorCoordinator.Terminate) {
            // Ready to Terminate
            readyToFinish = true;
        }
    }

    /**
     * Handle operator event if body IS {@link OperatorEventHandler}
     */
    public void handleOperatorEventWithBody(OperatorEvent evt) {
        operatorEventHandleBodyOperatorRef.handleOperatorEvent(evt);
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
