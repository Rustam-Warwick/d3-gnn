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
import elements.iterations.MessageCommunication;
import operators.iterations.FeedbackChannel;
import operators.iterations.FeedbackChannelBroker;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.operator.OperatorUtils;
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
import org.apache.flink.util.IOUtils;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Feedback consumer operator as a source operator
 */
public class IterationSourceOperator extends StreamSource<GraphOp, IterationSourceOperator.MySourceFunction>
        implements FeedbackConsumer<StreamRecord<GraphOp>> {

    private final IterationID iterationId; // Iteration Id is a unique id of the iteration. Can be shared by many producers

    private final short position; // Position in the GNN Chain

    private transient MailboxExecutor mailboxExecutor; // Mailbox for consuming iteration events

    private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel; // Channel to send feedbacks to

    private transient BroadcastOutput<GraphOp> broadcastOutput; // Special Output for broadcasting the elements

    private transient int numberOfElementsReceived;

    public IterationSourceOperator(IterationID iterationId, short position) {
        super(new MySourceFunction());
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
        getUserFunction().setFeedbackChannel(feedbackChannel);
    }

    @Override
    public void open() throws Exception {
        super.open();
        getProcessingTimeService().scheduleWithFixedDelay(new CheckTermination(), 45000, 45000);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        feedbackChannel.addSnapshot(checkpointId); // Stop all the iterations from being consumed
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        try {
            numberOfElementsReceived++;
            if (element.getValue().getMessageCommunication() == MessageCommunication.P2P) {
                output.collect(element);
            } else if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
                broadcastOutput.broadcastEmit(element);
            }
//            if(element.getValue().getElement() != null){
//                element.getValue().getElement().modifyNDArrayPossessionCounter(item -> item - 1);
//            }
        } catch (Exception e) {
            // Errors can happen here
        }
    }

    @Override
    public void finish() throws Exception {
        while(mailboxExecutor.tryYield()){
            Thread.sleep(300);
        };
        IOUtils.closeQuietly(feedbackChannel);
        super.finish();
    }

    protected static class MySourceFunction implements SourceFunction<GraphOp> {
        private transient FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel;

        public void setFeedbackChannel(FeedbackChannel<StreamRecord<GraphOp>> feedbackChannel) {
            this.feedbackChannel = feedbackChannel; // Reference to the outer FeedBack Channel
        }

        @Override
        public void run(SourceContext<GraphOp> ctx) throws Exception {
            while (!feedbackChannel.hasProducer()) {
                Thread.onSpinWait();
            }
            feedbackChannel.getPhaser().register();
            try{
                System.out.println("Enter register phase");
                feedbackChannel.getPhaser().awaitAdvanceInterruptibly(feedbackChannel.getPhaser().arrive());
                System.out.println("Existing register phase");
            }catch (InterruptedException e){
                System.out.println("Interrupted Closing the channel");
                IOUtils.closeQuietly(feedbackChannel);
            }
        }

        @Override
        public void cancel() {

        }
    }

    protected class CheckTermination implements ProcessingTimeService.ProcessingTimeCallback {
        @Override
        public void onProcessingTime(long time) throws Exception {
            if(numberOfElementsReceived == 0){
                System.out.println("Emitting Watermark");
                output.emitWatermark(new Watermark(Long.MAX_VALUE));
            }
            numberOfElementsReceived = 0;
        }
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
