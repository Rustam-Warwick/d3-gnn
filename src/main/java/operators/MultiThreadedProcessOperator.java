package operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Process Operator that distributes single parallelism across @link{nThreads} number of local threads.
 * Makes sure to close the operator once the Threads have stopped execution
 * Implementation uses task mailbox so that we can use extra CPU cores allocated to this slotSharingGroup
 *
 * @param <IN>
 * @param <OUT>
 * @implNote It is thread-safe until the userFunction is Thread Safe!!!
 */
public class MultiThreadedProcessOperator<IN, OUT> extends ProcessOperator<IN, OUT> implements BoundedOneInput {

    private final int nThreads;

//    private transient ThreadPoolExecutor executorService;

    private transient MailboxExecutor[] executors;

    private transient int currentMailbox; // Round robin counter

    private transient ThreadLocal<SynchronousCollector> collector;

    private transient ThreadLocal<ContextImpl> context;

    private transient LinkedBlockingQueue<Runnable> workQueue;


    public MultiThreadedProcessOperator(ProcessFunction<IN, OUT> function, int nThreads) {
        super(function);
        this.nThreads = nThreads; // Number of threads to dispatch for the job
        this.chainingStrategy = ChainingStrategy.HEAD; // When chaining is involved this operator does not close properly
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = ThreadLocal.withInitial(() -> new SynchronousCollector(output));
        context = ThreadLocal.withInitial(() -> new ContextImpl(userFunction, getProcessingTimeService()));
        workQueue = new LinkedBlockingQueue<>(nThreads);
        executors = new MailboxExecutor[nThreads];
        for (int i = 0; i < nThreads; i++) {
            executors[i] = getContainingTask().getMailboxExecutorFactory().createExecutor(TaskMailbox.MIN_PRIORITY);
        }

//        executorService = new ThreadPoolExecutor(nThreads, nThreads, Long.MAX_VALUE, TimeUnit.MILLISECONDS, workQueue);
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        workQueue.put(() -> {
            try {
                this.threadSafeProcessElement(element);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executors[currentMailbox].submit(()->workQueue.poll().run(),"Process Element");
        currentMailbox = (currentMailbox + 1) % nThreads;
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        for (int i = 0; i < executors.length; i++) {
            executors[i].tryYield();
        }
        super.processWatermark(mark);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        for (int i = 0; i < executors.length; i++) {
            executors[i].tryYield();
        }
        super.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        for (int i = 0; i < executors.length; i++) {
            executors[i].tryYield();
        }
        super.processWatermarkStatus(watermarkStatus);
    }

    private void threadSafeProcessElement(StreamRecord<IN> element) throws Exception {
        collector.get().getInnerCollector().setTimestamp(element);
        context.get().element = element;
        userFunction.processElement(element.getValue(), context.get(), collector.get());
        context.get().element = null;
    }

    @Override
    public void endInput() throws Exception {
        for (int i = 0; i < executors.length; i++) {
            executors[i].tryYield();
        }
    }

    private class SynchronousCollector implements Collector<OUT> {
        private final TimestampedCollector<OUT> innerCollector;

        public SynchronousCollector(Output<StreamRecord<OUT>> output) {
            this.innerCollector = new TimestampedCollector<OUT>(output);
        }

        public TimestampedCollector<OUT> getInnerCollector() {
            return innerCollector;
        }

        @Override
        public void collect(OUT record) {
            synchronized (MultiThreadedProcessOperator.this) {
                innerCollector.collect(record);
            }
        }

        @Override
        public void close() {
            //  Pass
        }
    }

    private class ContextImpl extends ProcessFunction<IN, OUT>.Context implements TimerService {
        private final ProcessingTimeService processingTimeService;
        private StreamRecord<IN> element;

        ContextImpl(
                ProcessFunction<IN, OUT> function, ProcessingTimeService processingTimeService) {
            function.super();
            this.processingTimeService = processingTimeService;
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
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }
            synchronized (MultiThreadedProcessOperator.this){
                output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
            }
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
        }

        @Override
        public TimerService timerService() {
            return this;
        }
    }
}
