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
import elements.Op;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.FeedbackChannelBroker;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Feedback operator send data to the Head Operator through a FeedbackChannel Broker
 * It also handles the checkpointing of the data @todo enable checkpointing
 */
public class SimpleTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<GraphOp, Void> {
    /**
     * Iteration id is a unique identifier of a particular iteration
     * Since multiple iteration sinks can be added to the same iteration head
     */
    private final IterationID iterationId;
    /**
     * We distinguish how the record is processed according to if objectReuse is enabled.
     */
    private transient Consumer<StreamRecord<GraphOp>> recordConsumer;

    private transient FeedbackChannel<StreamRecord<GraphOp>> channel;


    public SimpleTailOperator(IterationID iterationId) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.chainingStrategy = ChainingStrategy.ALWAYS;

    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void open() throws Exception {
        super.open();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.channel = broker.getChannel(key);
        getContainingTask().getConfiguration().getNumberOfNetworkInputs();
        this.recordConsumer =
                getExecutionConfig().isObjectReuseEnabled()
                        ? this::processIfObjectReuseEnabled
                        : this::processIfObjectReuseNotEnabled;
    }

    @Override
    public void processElement(StreamRecord<GraphOp> streamRecord) {
        recordConsumer.accept(streamRecord);
    }

//    @Override
//    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
//        super.prepareSnapshotPreBarrier(checkpointId);
//        GraphOp checkpointBarrier = new GraphOp();
//        checkpointBarrier.checkpointBarrier = checkpointId;
//        channel.put(new StreamRecord<>(checkpointBarrier));
//    }

//    @Override
//    public void notifyCheckpointAborted(long checkpointId) throws Exception {
//        super.notifyCheckpointAborted(checkpointId);
//
//        // TODO: Unfortunately, we have to rely on the tail operator to help
//        // abort the checkpoint since the task thread of the head operator
//        // might get blocked due to not be able to close the raw state files.
//        // We would try to fix it in the Flink side in the future.
//        SubtaskFeedbackKey<?> key =
//                OperatorUtils.createFeedbackKey(iterationId, feedbackIndex)
//                        .withSubTaskIndex(
//                                getRuntimeContext().getIndexOfThisSubtask(),
//                                getRuntimeContext().getAttemptNumber());
//        Checkpoints<?> checkpoints = CheckpointsBroker.get().getCheckpoints(key);
//        if (checkpoints != null) {
//            checkpoints.abort(checkpointId);
//        }
//    }

    private void processIfObjectReuseEnabled(StreamRecord<GraphOp> record) {
        // Since the record would be reused, we have to clone a new one
        GraphOp cloned = record.getValue().copy();
        channel.put(new StreamRecord<>(cloned, cloned.getTimestamp()));
    }

    private void processIfObjectReuseNotEnabled(StreamRecord<GraphOp> record) {
        // Since the record would not be reused, we could modify it in place.
        channel.put(new StreamRecord<>(record.getValue(), record.getValue().getTimestamp()));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        short iterationNumber = (short) (mark.getTimestamp() % 4);
        if (iterationNumber < 3) {
            // Still not terminated need to iterate still
            GraphOp watermark = new GraphOp(Op.WATERMARK, null, mark.getTimestamp());
            channel.put(new StreamRecord<>(watermark, mark.getTimestamp()));
        }
        // Else Pass
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(channel);
        super.close();
    }
}
