package operators;

import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.statefun.flink.core.feedback.*;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.OutputTag;

import java.util.Objects;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class GNNKeyedProcessOperator extends AbstractUdfStreamOperator<GraphOp, KeyedProcessFunction<String, GraphOp, GraphOp>>
        implements OneInputStreamOperator<GraphOp, GraphOp>, Triggerable<String, VoidNamespace>, FeedbackConsumer<StreamRecord<GraphOp>> {


    private static final long serialVersionUID = 1L;
    private final IterationID iterationId;
    private transient TimestampedCollector<GraphOp> collector;
    private transient GNNKeyedProcessOperator.ContextImpl context;
    private transient GNNKeyedProcessOperator.OnTimerContextImpl onTimerContext;
    private MailboxExecutor mailboxExecutor;


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
        collector = new TimestampedCollector<>(output);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new GNNKeyedProcessOperator.ContextImpl(userFunction, timerService);
        onTimerContext = new GNNKeyedProcessOperator.OnTimerContextImpl(userFunction, timerService);

        ((StreamingGNNLayerFunction) userFunction).collector = collector;
        ((StreamingGNNLayerFunction) userFunction).ctx = context;

        registerFeedbackConsumer(
                (Runnable runnable) -> {
                    mailboxExecutor.execute(runnable::run, "Head feedback");
                });
        super.open();
    }

    @Override
    public void onEventTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement(element.getValue(), context, collector);
        context.element = null;
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        setKeyContextElement(element);
        context.element = element;
        userFunction.processElement(element.getValue(), context, collector);
        context.element = null;
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<String, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
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



    private class ContextImpl extends KeyedProcessFunction<String, GraphOp, GraphOp>.Context {

        private final TimerService timerService;

        private StreamRecord<GraphOp> element;

        ContextImpl(KeyedProcessFunction<String, GraphOp, GraphOp> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }
            long timeStamp = Long.MIN_VALUE;
            if (Objects.nonNull(element)) {
                timeStamp = element.getTimestamp();
            }
            output.collect(outputTag, new StreamRecord<>(value, timeStamp));
        }

        @Override
        @SuppressWarnings("unchecked")
        public String getCurrentKey() {
            return (String) GNNKeyedProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl extends KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<String, VoidNamespace> timer;

        OnTimerContextImpl(KeyedProcessFunction<String, GraphOp, GraphOp> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public String getCurrentKey() {
            return timer.getKey();
        }
    }


}
