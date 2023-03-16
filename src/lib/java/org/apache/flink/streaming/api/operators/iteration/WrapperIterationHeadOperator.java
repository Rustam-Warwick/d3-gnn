package org.apache.flink.streaming.api.operators.iteration;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.LifeCycleControl;
import ai.djl.ndarray.NDManager;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
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
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.function.Consumer;

/**
 * HEAD logic wrapper around the main operator logic for {@link OneInputStreamOperator}
 * <p>
 *     Currently only supports single input stream operator but can be extended to support many sources as well
 *     When Long.MAX_VALUE watermark arrives enters into termination detection mode
 *     In this mode only mailbox messages are processed since no external messages are assumed
 *     To not block the subsequent iteration head operators in the pipeline while terminating it emits Long.MAX_VALUE - 1
 *     This watermark notifies that the input is actually finished and currently the system is in termination detection mode
 *     Once finished termination detection it emits the Long.MAX_VALUE watermark
 * </p>
 *
 * @param <OUT> Output Type
 */
public class WrapperIterationHeadOperator<OUT> implements StreamOperator<OUT>, OneInputStreamOperator<Object, OUT>, OperatorEventHandler {

    /**
     * Event dispatched map field
     */
    static final Field eventDispatcherMapField;

    static {
        try{
            eventDispatcherMapField = OperatorEventDispatcherImpl.class.getDeclaredField("handlers");
            eventDispatcherMapField.setAccessible(true);
        }catch (Exception e){
            throw new RuntimeException("Run off security mananger, need to access reflection");
        }
    }

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
     * Consumer for Request Scan events
     */
    protected final Consumer<WrapperIterationHeadOperatorCoordinator.TerminationScanRequest> requestScanConsumer;

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
    protected final OperatorEventHandler operatorEventHandlerBodyOperatorRef;

    /**
     * Just Reference to ExposingInternalTimerSerivce operator to avoid constant type casting
     */
    protected final ExposingInternalTimerService exposingInternalTimerServiceOperatorRef;

    /**
     * Operator IO metric
     */
    protected final OperatorIOMetricGroup operatorIOMetricGroup;

    /**
     * If the elements in this channel are instances of {@link ai.djl.ndarray.LifeCycleControl}. If it is the case, need to resume on taking from buffer
     */
    protected final boolean hasLifeCycleControl;

    /**
     * Link to {@link NDManager} to avoid constant access to {@link ThreadLocal}
     */
    protected final NDManager manager = BaseNDManager.getManager();

    /**
     * Termination Detection point reached can close this operator
     */
    protected boolean isTerminationReady;

    /**
     * Previous count used for termination detection
     */
    protected long terminationDetectionCounter;


    public WrapperIterationHeadOperator(int iterationID, MailboxExecutor mailboxExecutor, AbstractStreamOperator<OUT> bodyOperator, StreamOperatorParameters<OUT> parameters, boolean hasLifeCycleControl) {
        this.iterationID = iterationID;
        this.hasLifeCycleControl = hasLifeCycleControl;
        this.mailboxExecutor = mailboxExecutor;
        this.bodyOperator = bodyOperator;
        this.operatorIOMetricGroup = getMetricGroup().getIOMetricGroup();
        this.channelID = new IterationChannelKey(parameters.getContainingTask().getEnvironment().getJobID(), iterationID, parameters.getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber(), parameters.getContainingTask().getEnvironment().getTaskInfo().getIndexOfThisSubtask());
        this.operatorEventGateway = parameters.getOperatorEventDispatcher().getOperatorEventGateway(getOperatorID());
        try{
            // Underlying operator is initialized first, so it may have declared a dispatcher already. Need to override the handler and redirect messages to the underlying operator
            Map<OperatorID, OperatorEventHandler> handlerMap = (Map<OperatorID, OperatorEventHandler>) eventDispatcherMapField.get(parameters.getOperatorEventDispatcher());
            this.operatorEventHandlerBodyOperatorRef = handlerMap.put(getOperatorID(), this);
        }catch (Exception e){throw new RuntimeException("Cannot access the field");}
        this.oneInputBodyOperatorRef = (bodyOperator instanceof OneInputStreamOperator) ? (OneInputStreamOperator<Object, OUT>) bodyOperator : null;
        this.exposingInternalTimerServiceOperatorRef = (bodyOperator instanceof ExposingInternalTimerService) ? (ExposingInternalTimerService) bodyOperator : null;
        this.requestScanConsumer = exposingInternalTimerServiceOperatorRef == null ? this::handleRequestScanEventWithoutTimers : this::handleRequestScanEventWithTimers;
        this.operatorEventConsumer = operatorEventHandlerBodyOperatorRef == null ? this::handleOperatorEventSelf : this::handleOperatorEventWithBody;
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
            if (hasLifeCycleControl) ((LifeCycleControl) el.getValue()).resume();
            operatorIOMetricGroup.getNumRecordsInCounter().inc();
            setKeyContextElement(el);
            processElement(el);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // NDManager is delayed per batch of iteration messages, and these messages can be quite large, so it is good to delay them per element instead of per batch so no memory overflow occurs
            manager.resumeAndDelay();
        }
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
        IterationChannel<StreamRecord<Object>> channel = IterationChannelBroker.getBroker().getIterationChannel(channelID);
        bodyOperator.getContainingTask().getCancelables().registerCloseable(channel);
        channel.setConsumer(this::processOneInputFeedback, mailboxExecutor);
        bodyOperator.initializeState(streamTaskStateManager);
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
        if (mark.getTimestamp() == Long.MAX_VALUE && !isTerminationReady) {
            // Enter the termination loop
            oneInputBodyOperatorRef.processWatermark(new Watermark(Long.MAX_VALUE - 1));
            if (bodyOperator.getRuntimeContext().getIndexOfThisSubtask() == 0)
                operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.StartTermination());
            while (!isTerminationReady) {
                mailboxExecutor.yield();
            }
            oneInputBodyOperatorRef.processWatermark(mark); // Operators might depend on this watermark so send it
        }else{
            oneInputBodyOperatorRef.processWatermark(mark);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        oneInputBodyOperatorRef.processWatermarkStatus(watermarkStatus);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        oneInputBodyOperatorRef.processLatencyMarker(latencyMarker);
    }

    /**
     * Handle {@link WrapperIterationHeadOperatorCoordinator.TerminationScanRequest} events when there are no times in the body
     */
    final public void handleRequestScanEventWithoutTimers(WrapperIterationHeadOperatorCoordinator.TerminationScanRequest terminationScanRequest){
        long tmp = operatorIOMetricGroup.getNumRecordsInCounter().getCount() + operatorIOMetricGroup.getNumRecordsOutCounter().getCount();
        operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.TerminationScanResponse(
                tmp == terminationDetectionCounter
        ));
        terminationDetectionCounter = tmp;
    }

    /**
     * Handle {@link WrapperIterationHeadOperatorCoordinator.TerminationScanRequest} events when there are times in the body
     */
    final public void handleRequestScanEventWithTimers(WrapperIterationHeadOperatorCoordinator.TerminationScanRequest terminationScanRequest){
        final boolean[] hasTimers = new boolean[]{false};
        try {
            exposingInternalTimerServiceOperatorRef.getInternalTimerService().forEachProcessingTimeTimer((ns, timer) -> {
                hasTimers[0] = true;
                throw new Exception("Found, do not process rest");
            });
        } catch (Exception ignored) {}
        long tmp = operatorIOMetricGroup.getNumRecordsInCounter().getCount() + operatorIOMetricGroup.getNumRecordsOutCounter().getCount();
        operatorEventGateway.sendEventToCoordinator(new WrapperIterationHeadOperatorCoordinator.TerminationScanResponse(
                !hasTimers[0] && tmp == terminationDetectionCounter));
        terminationDetectionCounter = tmp;
    }

    /**
     * Handle operator event if body is NOT {@link OperatorEventHandler}
     */
    final public void handleOperatorEventSelf(OperatorEvent evt) {
        if (evt instanceof WrapperIterationHeadOperatorCoordinator.TerminationScanRequest) {
            // Requested scan for termination detection
            requestScanConsumer.accept((WrapperIterationHeadOperatorCoordinator.TerminationScanRequest) evt);
        } else if (evt instanceof WrapperIterationHeadOperatorCoordinator.TerminationReady) {
            // Ready to Terminate
            isTerminationReady = true;
        }
    }

    /**
     * Handle operator event if body IS {@link OperatorEventHandler}
     */
    final public void handleOperatorEventWithBody(OperatorEvent evt) {
        operatorEventHandlerBodyOperatorRef.handleOperatorEvent(evt);
        handleOperatorEventSelf(evt);
    }

    @Override
    final public void handleOperatorEvent(OperatorEvent evt) {
        operatorEventConsumer.accept(evt);
    }

}
