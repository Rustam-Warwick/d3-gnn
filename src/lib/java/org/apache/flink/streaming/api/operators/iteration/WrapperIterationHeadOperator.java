package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * HEAD Operator for handling Stream Iterations
 * Acts like a source
 * @todo Add Termination Detection
 * @param <OUT> Output Type
 */
public class WrapperIterationHeadOperator<OUT> implements StreamOperator<OUT>, OneInputStreamOperator<Object, OUT>, TwoInputStreamOperator<Object, Object, OUT> {

    /**
     * Unique ID for the Iteration
     */
    protected final int iterationID;

    /**
     * Mailbox Executor to attach to
     */
    protected final MailboxExecutor mailboxExecutor;

    /**
     * Main {@link StreamOperator} that is wrapped by this HEAD
     */
    protected final StreamOperator<OUT> bodyOperator;

    /**
     * Just References with OneInput type to avoid constant type casting
     */
    protected final OneInputStreamOperator<Object, OUT> oneInputBodyOperatorRef;

    /**
     * Just References with TwoInput type to avoid constant type casting
     */
    protected final TwoInputStreamOperator<Object,Object, OUT> twoInputBodyOperatorRef;

    public WrapperIterationHeadOperator(int iterationID, MailboxExecutor mailboxExecutor, StreamOperator<OUT> bodyOperator) {
        this.iterationID = iterationID;
        this.mailboxExecutor = mailboxExecutor;
        this.bodyOperator = bodyOperator;
        if(bodyOperator instanceof OneInputStreamOperator)oneInputBodyOperatorRef = (OneInputStreamOperator<Object, OUT>) bodyOperator;
        else oneInputBodyOperatorRef = null;
        if(bodyOperator instanceof TwoInputStreamOperator)twoInputBodyOperatorRef = (TwoInputStreamOperator<Object, Object, OUT>) bodyOperator;
        else twoInputBodyOperatorRef = null;
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

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {
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
        oneInputBodyOperatorRef.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        oneInputBodyOperatorRef.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        oneInputBodyOperatorRef.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processElement1(StreamRecord<Object> element) throws Exception {
        twoInputBodyOperatorRef.processElement1(element);
    }

    @Override
    public void processElement2(StreamRecord<Object> element) throws Exception {
        twoInputBodyOperatorRef.processElement2(element);
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        twoInputBodyOperatorRef.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        twoInputBodyOperatorRef.processWatermark2(mark);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        twoInputBodyOperatorRef.processLatencyMarker1(latencyMarker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        twoInputBodyOperatorRef.processLatencyMarker2(latencyMarker);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        twoInputBodyOperatorRef.processWatermarkStatus1(watermarkStatus);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        twoInputBodyOperatorRef.processWatermarkStatus2(watermarkStatus);
    }
}
