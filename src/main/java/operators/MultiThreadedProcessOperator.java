package operators;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public class MultiThreadedProcessOperator<IN, OUT> extends ProcessOperator<IN, OUT> {

    private final int nThreads;

    private transient ThreadPoolExecutor executorService;

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
        workQueue = new LimitedBlockingQueue<>((int) (nThreads * 1.2));
        executorService = new ThreadPoolExecutor(nThreads, nThreads, Long.MAX_VALUE, TimeUnit.MILLISECONDS, workQueue);
        Thread.sleep(1000);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        executorService.submit(() -> {
            try {
                this.threadSafeProcessElement(element);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }); // Waiting if the buffer is full
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if(executorService.isShutdown())super.processWatermark(mark);
        executorService.submit(() -> {
            try {
                synchronized (this) {
                    super.processWatermark(mark);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        if(executorService.isShutdown())super.processLatencyMarker(latencyMarker);
        executorService.submit(() -> {
            try {
                synchronized (this) {
                    super.processLatencyMarker(latencyMarker);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if(executorService.isShutdown())super.processWatermarkStatus(watermarkStatus);
        executorService.submit(() -> {
            try {
                synchronized (this) {
                    super.processWatermarkStatus(watermarkStatus);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void threadSafeProcessElement(StreamRecord<IN> element) throws Exception {
        collector.get().getInnerCollector().setTimestamp(element);
        context.get().element = element;
        userFunction.processElement(element.getValue(), context.get(), collector.get());
        context.get().element = null;
    }

    @Override
    public void finish() throws Exception {
        System.out.println("Finishing Mutli Threaded");
        executorService.shutdown();
        while (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
            Thread.onSpinWait();
        }

        System.out.println("Finished Mutli Threaded");
        super.finish();
    }

    private static class LimitedBlockingQueue<E> extends LinkedBlockingQueue<E> {
        public LimitedBlockingQueue() {
        }

        public LimitedBlockingQueue(int capacity) {
            super(capacity);
        }

        public LimitedBlockingQueue(Collection<? extends E> c) {
            super(c);
        }

        @Override
        public boolean offer(@NotNull E e) {
            try {
                put(e);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
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
            synchronized (MultiThreadedProcessOperator.this) {
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
