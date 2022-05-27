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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.checkpoint.Checkpoints;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.statefun.flink.core.logger.Loggers;
import org.apache.flink.statefun.flink.core.logger.UnboundedFeedbackLoggerFactory;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Feedback operator send data to the Head Operator through a FeedbackChannel Broker
 * It also handles the checkpointing of the data @todo enable checkpointing
 */
public class IterationTailOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<GraphOp, Void>, BoundedOneInput {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    protected transient KeySelector<?, ?> stateKeySelector; // Exists in AbstractStreamOperator but is private so re-define here

    private transient Checkpoints<StreamRecord<GraphOp>> checkpoints;

    private OperatorID operatorID; // Unique Operator Id

    private transient Consumer<StreamRecord<GraphOp>> recordConsumer; // Depending on the Object Reuse enabled or not

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to


    public IterationTailOperator(IterationID iterationId) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
//        UnboundedFeedbackLoggerFactory<StreamRecord<GraphOp>> feedbackLoggerFactory =
//                (UnboundedFeedbackLoggerFactory<StreamRecord<GraphOp>>)
//                        Loggers.unboundedSpillableLoggerFactory(
//                                getContainingTask().getEnvironment().getIOManager(),
//                                getRuntimeContext().getMaxNumberOfParallelSubtasks(),
//                                totalMemoryUsedForFeedbackCheckpointing,
//                                elementSerializer,
//                                keySelector);

//        this.checkpoints = new org.apache.flink.statefun.flink.core.feedback.Checkpoints<>(feedbackLoggerFactory::create);
        operatorID = getOperatorID();
        registerFeedbackWriter();
        this.recordConsumer =
                getExecutionConfig().isObjectReuseEnabled()
                        ? this::processIfObjectReuseEnabled
                        : this::processIfObjectReuseNotEnabled;
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        this.stateKeySelector = config.getStatePartitioner(0, getUserCodeClassloader());
    }

    @Override
    public void processElement(StreamRecord<GraphOp> streamRecord) {
        recordConsumer.accept(streamRecord);
    }

    @Override
    public void endInput() throws Exception {

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
//        super.prepareSnapshotPreBarrier(checkpointId);
//        GraphOp checkpointBarrier = new GraphOp();
//        checkpointBarrier.checkpointBarrier = checkpointId;
//        channel.put(new StreamRecord<>(checkpointBarrier));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        feedbackChannel.producerActivate(operatorID);
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        feedbackChannel.producerActivate(operatorID);
        super.notifyCheckpointAborted(checkpointId);
    }

    private void registerFeedbackWriter(){
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> realKey =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.feedbackChannel = broker.getChannel(realKey);
        feedbackChannel.registerPublisher(operatorID);
    }

    private void processIfObjectReuseEnabled(StreamRecord<GraphOp> record) {
        // Since the record would be reused, we have to clone a new one
        GraphOp cloned = record.getValue().copy();
        feedbackChannel.put(new StreamRecord<>(cloned, record.getTimestamp()), operatorID);
    }

    private void processIfObjectReuseNotEnabled(StreamRecord<GraphOp> record) {
        // Since the record would not be reused, we could modify it in place.
        feedbackChannel.put(new StreamRecord<>(record.getValue(), record.getTimestamp()), operatorID);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(feedbackChannel);
        super.close();
    }
}
