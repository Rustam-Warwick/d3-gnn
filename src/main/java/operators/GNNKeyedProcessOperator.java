package operators;

import elements.GraphOp;
import elements.Op;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.broadcast.BroadcastOutput;
import org.apache.flink.iteration.broadcast.BroadcastOutputFactory;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * This operator is tightly coupled with
 *
 * @implNote Only use for the first GNN Layer that consume external updates
 * @see StreamingGNNLayerFunction
 */
public class GNNKeyedProcessOperator extends KeyedProcessOperator<String, GraphOp, GraphOp> implements FeedbackConsumer<StreamRecord<GraphOp>> {
    private static final long serialVersionUID = 1L;
    private final IterationID iterationId;
    private transient List<String> thisOperatorKeys;
    private MailboxExecutor mailboxExecutor;
    private boolean watermarkInIteration = false;
    private Long waitingWatermark = null;
    private long iterationStartTime = 0;


    public GNNKeyedProcessOperator(StreamingGNNLayerFunction function, IterationID iterationId) {
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
        thisOperatorKeys = getThisOperatorKeys();
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
        if (element.getValue().op == Op.WATERMARK) {
            short iterationNumber = (short) (element.getTimestamp() % 4);
            Watermark newWatermark = new Watermark(element.getTimestamp() + 1);
            element.setTimestamp(newWatermark.getTimestamp());
            element.getValue().setTimestamp(newWatermark.getTimestamp());
            for(String key: thisOperatorKeys){
                setCurrentKey(key);
                processElement(element);
            }
            if (iterationNumber < 2) {
                // Still need to traverse the stream before updating the timer
                output.emitWatermark(newWatermark);
            } else {
                // Watermark is ready to be consumed, before consuming do onWatermark on all the keyed elements
                super.processWatermark(newWatermark);
                watermarkInIteration = false;
                System.out.format("Time taken to complete watermark sync is %s\n", getRuntimeContext().getProcessingTimeService().getCurrentProcessingTime() - iterationStartTime);
                iterationStartTime = 0;
                if(waitingWatermark != null){
                    processWatermark(new Watermark(waitingWatermark));
                    waitingWatermark = null;
                }
            }
        } else {
            setKeyContextElement(element);
            processElement(element);
        }
    }


    /**
     * Watermarks received should be three times all-reduces in this layer
     * This ensures consistency for longest graph operation
     * Actual watermarks are sent in processFeedback function
     * Also calls ingests PRE_WATERMARK event into the stream
     * @param mark Watermark
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if(watermarkInIteration){
            if(waitingWatermark == null) waitingWatermark = mark.getTimestamp();
            else waitingWatermark = Math.max(waitingWatermark, mark.getTimestamp());
        }else {
            iterationStartTime = getRuntimeContext().getProcessingTimeService().getCurrentProcessingTime();
            Watermark iterationWatermark = new Watermark(mark.getTimestamp() - (mark.getTimestamp() % 4));
            GraphOp preWatermark = new GraphOp(Op.WATERMARK, null, iterationWatermark.getTimestamp());
            StreamRecord<GraphOp> element = new StreamRecord<>(preWatermark, iterationWatermark.getTimestamp());
            for (String key : thisOperatorKeys) {
                setCurrentKey(key);
                processElement(element);
            }
            output.emitWatermark(iterationWatermark); // Only output, do not register it
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

    private List<String> getThisOperatorKeys() {
        List<String> thisKeys = new ArrayList<>();
        int index = getRuntimeContext().getIndexOfThisSubtask();
        int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        for (short i = 0; i < maxParallelism; i++) {
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
            if (operatorIndex == index) {
                thisKeys.add(String.valueOf(i));
            }
        }
        return thisKeys;
    }

}
