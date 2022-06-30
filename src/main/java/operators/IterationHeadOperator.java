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
import elements.iterations.MessageCommunication;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Feedback consumer operator
 * It also handles the checkpointing of the data @todo enable checkpointing
 */
public class IterationHeadOperator extends AbstractStreamOperator<GraphOp>
        implements OneInputStreamOperator<GraphOp, GraphOp>, BoundedOneInput, FeedbackConsumer<StreamRecord<GraphOp>> {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    private final short position; // Position in the GNN Chain

    private transient MailboxExecutor mailboxExecutor; // Mailbox for consuming iteration events

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    private transient BroadcastOutput<GraphOp> broadcastOutput; // Special Output for broadcasting the elements

    public IterationHeadOperator(IterationID iterationId, short position) {
        this.iterationId = Objects.requireNonNull(iterationId);
        this.position = position;
        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<GraphOp>> output) {
        super.setup(containingTask, config, output);
        broadcastOutput =
                BroadcastOutputFactory.createBroadcastOutput(
                        output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
        mailboxExecutor = getContainingTask().getMailboxExecutorFactory().createExecutor(TaskMailbox.MIN_PRIORITY);
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        processElement(element);
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getMessageCommunication() == MessageCommunication.P2P) {
            output.collect(element);
        } else if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
            broadcastOutput.broadcastEmit(element);
        }

        LifeCycleNDManager.getInstance().clean();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        feedbackChannel.addSnapshot(checkpointId); // Stop all the iterations from being consumed
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void endInput() throws Exception {
        while (!feedbackChannel.allChannelsFinished()) {
            mailboxExecutor.tryYield();
            Thread.sleep(800);
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(feedbackChannel);
        super.close();
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if (position == 0)
            return; // Do not process watermark status from external streams since it is vital in training
        super.processWatermarkStatus(watermarkStatus);
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, getRuntimeContext().getAttemptNumber());
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        this.feedbackChannel = broker.getChannel(key);
        feedbackChannel.registerConsumer(this, mailboxExecutor);
    }

}
