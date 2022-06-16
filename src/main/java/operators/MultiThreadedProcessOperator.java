package operators;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Process Operator that distributes single parallelism across @link{nThreads} number of threads
 * @implNote It is thread-safe until the userFunction is Thread Safe!!!
 * @param <IN>
 * @param <OUT>
 */
public class MultiThreadedProcessOperator<IN,OUT> extends ProcessOperator<IN, OUT> {
    private final int nThreads;

    private transient ThreadPoolExecutor executorService;

    private transient ThreadLocal<SynchronousCollector<OUT>> collector;

    private transient ThreadLocal<ContextImpl> context;

    private transient Object lock;

    private transient LinkedBlockingDeque<Runnable> workQueue;



    public MultiThreadedProcessOperator(ProcessFunction<IN, OUT> function, int nThreads) {
        super(function);
        this.nThreads = nThreads;
    }

    @Override
    public void open() throws Exception {
        super.open();
        lock = new Object();
        collector = ThreadLocal.withInitial(()->new SynchronousCollector<OUT>(new TimestampedCollector<>(output), lock));
        context = ThreadLocal.withInitial(()->new ContextImpl(userFunction, getProcessingTimeService()));
        workQueue = new LinkedBlockingDeque<>();
        executorService = new ThreadPoolExecutor(nThreads, nThreads,10, TimeUnit.SECONDS, workQueue);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        executorService.submit(()->{
            try {
                this.threadSafeProcessElement(element);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void threadSafeProcessElement(StreamRecord<IN> element) throws Exception{
        collector.get().getInnerCollector().setTimestamp(element);
        context.get().element = element;
        userFunction.processElement(element.getValue(), context.get(), collector.get());
        context.get().element = null;
    }

    @Override
    public void finish() throws Exception {
        while(!workQueue.isEmpty()){
            Thread.sleep(1500); // Ensure no waiting requests
        }
        executorService.shutdown();
        while(!executorService.isShutdown() || !executorService.isTerminated() || executorService.getActiveCount() > 0){
            Thread.sleep(1500); // Ensure all Thread have finished processing
        }
        super.finish();
    }

    private static class SynchronousCollector<OUT> implements Collector<OUT> {
        private final TimestampedCollector<OUT> innerCollector;
        private final Object lock;

        public SynchronousCollector(TimestampedCollector<OUT> innerCollector, Object lock) {
            this.innerCollector = innerCollector;
            this.lock = lock;
        }

        public TimestampedCollector<OUT> getInnerCollector() {
            return innerCollector;
        }

        @Override
        public void collect(OUT record) {
            synchronized (lock){
                innerCollector.collect(record);
            }
        }

        @Override
        public void close() {
            //  Pass
        }
    }
    private class ContextImpl extends ProcessFunction<IN, OUT>.Context implements TimerService {
        private StreamRecord<IN> element;

        private final ProcessingTimeService processingTimeService;

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
            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark();
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
