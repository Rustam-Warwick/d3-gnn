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

import ai.djl.ndarray.NDArray;
import elements.GraphOp;
import elements.iterations.MessageCommunication;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.FeedbackKey;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Feedback consumer operator as a source operator
 *
 * @implNote <strong>Termination Detection</strong> is carried by looking at the operator input and output rate, periodically
 */
public class IterationSourceOperator extends StreamSource<GraphOp, IterationSourceOperator.MySourceFunction>
        implements FeedbackConsumer<StreamRecord<GraphOp>> {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    private transient MailboxExecutor mailboxExecutor; // Mailbox for consuming iteration events

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    private transient BroadcastOutput<GraphOp> broadcastOutput; // Special Output for broadcasting the elements

    private boolean timerRegistered = false; // While processing first message register termination timer, do not register afterward

    private boolean isBufferPoolClosed = false; // If buffer pool is closed there might be a race condition

    public IterationSourceOperator(IterationID iterationId) {
        super(new MySourceFunction());
        this.iterationId = Objects.requireNonNull(iterationId);
        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    /**
     * Register Consumer
     */
    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<GraphOp>> output) {
        super.setup(containingTask, config, output);
        broadcastOutput =
                BroadcastOutputFactory.createBroadcastOutput(
                        output, metrics.getIOMetricGroup().getNumRecordsOutCounter());
        mailboxExecutor = getContainingTask().getMailboxExecutorFactory().createExecutor(TaskMailbox.MAX_PRIORITY);
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
        getUserFunction().setFeedbackChannel(feedbackChannel);
    }

    /**
     * Add Snapshot to all queues
     */
    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        feedbackChannel.addSnapshot(checkpointId); // Stop all the iterations from being consumed
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    /**
     * Process the Feedback
     */
    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        if (isBufferPoolClosed) return;
        if (!timerRegistered) {
            getRuntimeContext().getProcessingTimeService().scheduleWithFixedDelay(new CheckTermination(), 5000, 10000);
            timerRegistered = true;
        }
        try {

            if (element.getValue().getMessageCommunication() == MessageCommunication.P2P) {
                output.collect(element);
            } else if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
                broadcastOutput.broadcastEmit(element);
            }
        } catch (CancelTaskException bufferPool) {
            isBufferPoolClosed = true;
            BaseWrapperOperator.LOG.error(bufferPool.getMessage());
        } catch (Exception e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
            BaseWrapperOperator.LOG.error(element.getValue().toString());
        } finally {
            if (element.getValue().getElement() != null) {
                element.getValue().getElement().applyForNDArrays(NDArray::prepone);
            }
        }
    }

    /**
     * Only happens if the job has fully completed
     * Wait maybe some pending messages
     * Emit everything that remains
     */
    @Override
    public void finish() throws Exception {
        BaseWrapperOperator.LOG.info(String.format("Finishing %s", getRuntimeContext().getTaskNameWithSubtasks()));
        while (mailboxExecutor.tryYield()) {
            Thread.sleep(5000); // Wait maybe more messages are to come
        }
        IOUtils.closeQuietly(feedbackChannel);
        BaseWrapperOperator.LOG.info(String.format("Finished %s", getRuntimeContext().getTaskNameWithSubtasks()));
        super.finish();
    }

    /**
     * Register the consumer
     */
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

    /**
     * Actual Source function to be executed
     * Note that This is working in a separate Thread from the functions of the operator.
     * This actually lives in the Task's Thread not Operator Thread
     */
    protected static class MySourceFunction implements SourceFunction<GraphOp> {
        private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel;

        public void setFeedbackChannel(FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel) {
            this.feedbackChannel = feedbackChannel; // Reference to the outer FeedBack Channel
        }

        @Override
        public void run(SourceContext<GraphOp> ctx) throws Exception {
            try {
                while (!feedbackChannel.hasProducer()) {
                    // Make sure there are available producers, otherwise will skip directly to cancellation
                    Thread.sleep(500);
                }
                feedbackChannel.getPhaser().register(); // Register here since they are different threads
                feedbackChannel.getPhaser().awaitAdvanceInterruptibly(feedbackChannel.getPhaser().arrive());
            } catch (InterruptedException e) {
                // Close the channel here to not have any remaining message after this exists from interruption
                IOUtils.closeQuietly(feedbackChannel); // Close this channel here, because this operator is always entering to finish() method even if interrupted
                BaseWrapperOperator.LOG.error("Interrupted Closing the source");
            }
        }

        /**
         * Channel closed preemtively
         */
        @Override
        public void cancel() {
        }
    }

    /**
     * Find the termination of the iteration tails
     */
    protected class CheckTermination implements ProcessingTimeService.ProcessingTimeCallback {
        private final byte RETRY_COUNT = 3;
        private byte count = 0;
        private Long prevCount = null;

        @Override
        public void onProcessingTime(long time) throws Exception {
            if (count >= RETRY_COUNT) return; // Already did what it had to do
            long sumMessageCount = feedbackChannel.getTotalFlowingMessagesRate();
            // Operator has started so try to find termination point
            if (prevCount == null || sumMessageCount > prevCount) {
                count = 0;
                prevCount = sumMessageCount;
            } else {
                if (++count == RETRY_COUNT) {
                    BaseWrapperOperator.LOG.info(String.format("Watermark Emitted %s", getRuntimeContext().getTaskNameWithSubtasks()));
                    output.emitWatermark(new Watermark(Long.MAX_VALUE));
                }
            }
        }
    }
}
