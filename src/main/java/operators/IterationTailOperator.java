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

import elements.GraphOp;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
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
import org.apache.flink.util.IOUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Feedback operator send data to the Head Operator through a FeedbackChannel Broker
 * It also handles the checkpointing of the data @todo enable checkpointing
 */
public class IterationTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<GraphOp, Void> {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    private OperatorID operatorID; // Unique Operator Id

    private transient Consumer<StreamRecord<GraphOp>> recordConsumer; // Depending on the Object Reuse enabled or not

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    private transient TypeSerializer<StreamElement> recordSerializer; // StreamElement serializer

    private transient ListState<StreamRecord<GraphOp>> bufferedRecords; // Buffered records for the checkpoint

    public IterationTailOperator(IterationID iterationId) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

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

    @Override
    public void open() throws Exception {
        while (!feedbackChannel.hasConsumer()) {
            Thread.sleep(100);
        }
        super.open();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if(mark.getTimestamp() == Long.MAX_VALUE) feedbackChannel.finishChannel(operatorID);
        super.processWatermark(mark);
    }

    @Override
    public void close() throws Exception {
        feedbackChannel.finishChannel(operatorID);
        super.close();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<StreamRecord<GraphOp>> descriptor =
                new ListStateDescriptor(
                        "buffered-records",
                        recordSerializer); // Make sure the typeserializer works

        bufferedRecords = context.getOperatorStateStore().getListState(descriptor);
        if (Objects.nonNull(bufferedRecords)) {
            // Re-stream the iteration messages
            Iterable<StreamRecord<GraphOp>> tmp = bufferedRecords.get();
            for (StreamRecord<GraphOp> record : tmp) {
                processElement(record);
            }
        }
    }


    @Override
    public void processElement(StreamRecord<GraphOp> streamRecord) {
//        if (streamRecord.getValue().getElement() != null)
//            streamRecord.getValue().getElement().modifyNDArrayPossessionCounter(item->item + 1);
        recordConsumer.accept(streamRecord);
    }



    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        ArrayDeque<StreamRecord<GraphOp>> buffer = feedbackChannel.getUnsafeBuffer(operatorID);
        List<StreamRecord<GraphOp>> tmp = new ArrayList<>(buffer);
        bufferedRecords.update(tmp);
        super.snapshotState(context);
        feedbackChannel.finishSnapshot(context.getCheckpointId(), operatorID);
    }

    private void registerFeedbackWriter() {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> realKey =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        feedbackChannel = broker.getChannel(realKey);
        feedbackChannel.registerPublisher(operatorID);
    }

    private void processIfObjectReuseEnabled(StreamRecord<GraphOp> record) {
        // Since the record would be reused, we have to clone a new one
        feedbackChannel.put(record.copy(record.getValue()), operatorID);
    }

    private void processIfObjectReuseNotEnabled(StreamRecord<GraphOp> record) {
        // Since the record would not be reused, we could modify it in place.
        feedbackChannel.put(record, operatorID);
    }
}
