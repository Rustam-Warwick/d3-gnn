package operators;

import elements.GraphOp;
import elements.Op;
import functions.gnn_layers.CoStreamingGNNLayerFunction;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;

/**
 * This operator is tightly coupled with
 * @see StreamingGNNLayerFunction
 * This operator handles the insertion of data to StreamingGNNLayerFunction +
 * Registers MailboxExecutor for consuming iterative events
 */
public class GNNKeyedCoProcessOperator extends KeyedCoProcessOperator<String, GraphOp, GraphOp, GraphOp> implements FeedbackConsumer<StreamRecord<GraphOp>> {
    private static final long serialVersionUID = 1L;
    private final IterationID iterationId;
    private MailboxExecutor mailboxExecutor;

    public GNNKeyedCoProcessOperator(CoStreamingGNNLayerFunction function, IterationID iterationId) {
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
                KeyedCoProcessOperator.class.getDeclaredField("collector");
        collector.setAccessible(true);
        Field context =
                KeyedCoProcessOperator.class.getDeclaredField("context");
        context.setAccessible(true);

        ((CoStreamingGNNLayerFunction) userFunction).collector = (Collector<GraphOp>) collector.get(this);
        ((CoStreamingGNNLayerFunction) userFunction).ctx = (KeyedCoProcessFunction.Context) context.get(this);
        collector.setAccessible(false);
        context.setAccessible(false);
        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        if(element.getValue().op == Op.WATERMARK){
            long iterationNumber = WatermarkFilterOperator.getIterationNumber(element.getTimestamp());
            System.out.format("WATERMARK - %s at head \n",iterationNumber);
            if(iterationNumber < 3){
                // Still need to traverse the stream
                Watermark newWatermark = new Watermark(WatermarkFilterOperator.setIterationNumber(element.getTimestamp(), iterationNumber + 1));
                output.emitWatermark(newWatermark);
            }else{
                // Watermark is ready to be consumed
                Watermark mark = new Watermark(WatermarkFilterOperator.decode(element.getTimestamp()));
                super.processWatermark(mark);
                ((CoStreamingGNNLayerFunction)userFunction).onWatermark(mark);
            }
        }else{
            setKeyContextElement1(element);
            processElement1(element);
        }
    }

    /**
     * Watermarks received are a
     * @param mark
     * @throws Exception
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {

    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {

    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {

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
