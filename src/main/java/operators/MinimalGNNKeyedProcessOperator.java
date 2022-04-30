package operators;

import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import helpers.IteratingWatermarkUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Minimal version of GNNKeyedProcessOperator, which extends directly from the KeyedProcessOperator
 */
public class MinimalGNNKeyedProcessOperator extends KeyedProcessOperator<String, GraphOp, GraphOp> implements FeedbackConsumer<StreamRecord<GraphOp>> {
    private static final long serialVersionUID = 1L;
    private final IterationID iterationId;
    private MailboxExecutor mailboxExecutor;


    public MinimalGNNKeyedProcessOperator(StreamingGNNLayerFunction function, IterationID iterationId) {
        super(function);
        this.iterationId = iterationId;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<GraphOp>> output) {
        super.setup(containingTask, config, output);
        mailboxExecutor = containingTask.getMailboxExecutorFactory().createExecutor(TaskMailbox.MIN_PRIORITY); // mailboxExecutor for Iterations
    }

    @Override
    public void open() throws Exception {
        super.open();
        Field collector =
                KeyedProcessOperator.class.getDeclaredField("collector");
        collector.setAccessible(true);
        Field context =
                KeyedProcessOperator.class.getDeclaredField("context");
        context.setAccessible(true);


        ((StreamingGNNLayerFunction) userFunction).collector = (Collector<GraphOp>) collector.get(this);
        ((StreamingGNNLayerFunction) userFunction).ctx = (KeyedProcessFunction.Context) context.get(this);
        collector.setAccessible(false);
        context.setAccessible(false);
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        setKeyContextElement(element);
        processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        long watermarkState = IteratingWatermarkUtils.getIterativeWatermark(mark);
        if(watermarkState == 0){
            // This is external watermark, need to send it thrise in the stream
            Watermark iterationWatermark = IteratingWatermarkUtils.generateNewWatermarkIteration(mark, 4);
            output.emitWatermark(iterationWatermark);
        }else if(watermarkState == 1){
            // Watermark is correct now
        }else{
            // Iterate
        }
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int attemptNum = getRuntimeContext().getAttemptNumber();
        FeedbackKey<StreamRecord<GraphOp>> feedbackKey =
                OperatorUtils.createFeedbackKey(iterationId, 0);
        SubtaskFeedbackKey<StreamRecord<GraphOp>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<StreamRecord<GraphOp>> channel = broker.getChannel(key);
        OperatorUtils.registerFeedbackConsumer(channel, this, mailboxExecutor);
    }



}
