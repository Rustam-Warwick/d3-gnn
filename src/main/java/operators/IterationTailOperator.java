/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package operators;

import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.GraphOp;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Feedback operator send data to the Head Operator through a FeedbackChannel Broker
 * Also handles the fault-tolerance and checkpointing
 */
public class IterationTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<GraphOp, Void> {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    private OperatorID operatorID; // Unique Operator Id

    private transient Consumer<StreamRecord<GraphOp>> recordConsumer; // Depending on the Object Reuse enabled or not

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    private transient TypeSerializer<StreamElement> recordSerializer; // StreamElement serializer

    private transient ListState<StreamRecord<GraphOp>> bufferedRecords; // Buffered records for the checkpointing the state

    public IterationTailOperator(IterationID iterationId) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    /**
     * Register Feedback Writer
     */
    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        operatorID = getOperatorID();
        recordSerializer = new StreamElementSerializer<StreamRecord<GraphOp>>(getContainingTask().getConfiguration().getTypeSerializerIn(0, getClass().getClassLoader()));
        recordConsumer =
                getExecutionConfig().isObjectReuseEnabled()
                        ? this::processIfObjectReuseEnabled
                        : this::processIfObjectReuseNotEnabled;
        registerFeedbackWriter();
    }

    /**
     * Get buffered records if it is a restoed state also wait for the <strong>Consumer</strong>
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        while (!feedbackChannel.hasConsumer()) {
            Thread.sleep(500);
        }
        ListStateDescriptor<StreamRecord<GraphOp>> descriptor =
                new ListStateDescriptor(
                        "buffered-records",
                        recordSerializer); // Make sure the typeserializer works

        bufferedRecords = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            // Re-stream the iteration messages
            Iterable<StreamRecord<GraphOp>> tmp = bufferedRecords.get();
            for (StreamRecord<GraphOp> record : tmp) {
                processElement(record);
            }
        }
    }

    /**
     * Update this channel snapshot id
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        ArrayDeque<StreamRecord<GraphOp>> buffer = feedbackChannel.getUnsafeBuffer(operatorID);
        List<StreamRecord<GraphOp>> tmp = new ArrayList<>(buffer);
        bufferedRecords.update(tmp);
        super.snapshotState(context);
        feedbackChannel.finishSnapshot(context.getCheckpointId(), operatorID);
    }

    /**
     * Phaser arrive if this is max Watermark
     * Since Iteration Source will emit watermark to detect termination
     * Source will still try to finalize after this point so this is not immediately closed
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() == Long.MAX_VALUE) {
            BaseWrapperOperator.LOG.info(String.format("Watermark Arrived %s", getRuntimeContext().getTaskNameWithSubtasks()));
            feedbackChannel.getPhaser().arrive();
        }
        super.processWatermark(mark);
    }

    /**
     * Send to record consumer
     */
    @Override
    public void processElement(StreamRecord<GraphOp> streamRecord) {
        if(streamRecord.getValue().getElement() != null){
            streamRecord.getValue().getElement().applyForNDArrays(item-> LifeCycleNDManager.getInstance().postpone(item));
        }
        recordConsumer.accept(streamRecord);
    }

    /**
     * Since the record would be reused, we have to clone a new one
     */
    private void processIfObjectReuseEnabled(StreamRecord<GraphOp> record) {
        feedbackChannel.put(record.copy(record.getValue()), operatorID);
    }

    /**
     * Since the record would not be reused, we could modify it in place.
     */
    private void processIfObjectReuseNotEnabled(StreamRecord<GraphOp> record) {
        feedbackChannel.put(record, operatorID);
    }

    /**
     * Register Feedback Consumer + Register Metric Group + Register for the Phaser
     */
    private void registerFeedbackWriter() {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> realKey =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        feedbackChannel = broker.getChannel(realKey);
        feedbackChannel.getPhaser().register();
        Tuple2<Meter, Meter> meters = Tuple2.of(((InternalOperatorMetricGroup) getRuntimeContext().getMetricGroup()).getIOMetricGroup().getNumRecordsInRateMeter(),((InternalOperatorMetricGroup) getRuntimeContext().getMetricGroup()).getIOMetricGroup().getNumRecordsOutRate());
        feedbackChannel.registerPublisher(operatorID, meters);
    }
}
