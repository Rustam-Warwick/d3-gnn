package operators;

import elements.GraphOp;
import functions.StreamingGNNLayerFunction;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class GNNKeyedProcessOperator extends AbstractUdfStreamOperator<GraphOp, KeyedProcessFunction<String, GraphOp, GraphOp>>
        implements OneInputStreamOperator<GraphOp, GraphOp>, Triggerable<String, VoidNamespace> {


    private static final long serialVersionUID = 1L;

    private transient TimestampedCollector<GraphOp> collector;

    private transient GNNKeyedProcessOperator.ContextImpl context;

    private transient GNNKeyedProcessOperator.OnTimerContextImpl onTimerContext;

    public GNNKeyedProcessOperator(KeyedProcessFunction<String, GraphOp, GraphOp> function) {
        super(function);

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    public GNNKeyedProcessOperator(StreamingGNNLayerFunction function) {
        super(function);
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
        short thisMaster = this.setOperatorKeys();
        setCurrentKey(String.valueOf(thisMaster));
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

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<String, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    public Short setOperatorKeys() {
        int index = getRuntimeContext().getIndexOfThisSubtask();
        int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        boolean[] seen = new boolean[parallelism];
        List<Short> thisKeysList = new ArrayList<>(); // Keys of this operator
        List<Short> replicaKeysList = new ArrayList<>(); // Replica master keys


        for (short i = 0; i < maxParallelism; i++) {
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
            if (operatorIndex == index) {
                thisKeysList.add(i);
            } else if (!seen[operatorIndex]) {
                replicaKeysList.add(i);
            }

            seen[operatorIndex] = true;
        }

        ((StreamingGNNLayerFunction) userFunction).thisParts = thisKeysList;
        ((StreamingGNNLayerFunction) userFunction).replicaMasterParts = replicaKeysList;
        ((StreamingGNNLayerFunction) userFunction).currentPart = thisKeysList.get(0);
        ((StreamingGNNLayerFunction) userFunction).masterPart = thisKeysList.get(0);

        return thisKeysList.get(0);
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

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
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
