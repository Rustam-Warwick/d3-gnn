package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;
import org.jctools.queues.unpadded.SpscChunkedUnpaddedArrayQueue;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * <p>
 * This is the main in-memory channel that follows MPSC approach
 * MPSC Queue is implemented by maintaining multiple SPSC queues, one per producer
 * </p>
 *
 * @param <T> Type of elements iterating in this channel
 */
public class IterationChannel<T> implements Closeable {

    /**
     * List of producers identified by their IDs
     */
    private final Map<OperatorID, IterationQueue<T>> producers = new HashMap<>(5);

    /**
     * ID of this channel
     */
    private final IterationChannelKey channelKey;

    /**
     * Consumer & Executor of this channel, changes are made entirely for volatile to take effect
     */
    private final Tuple2<Consumer<T>, MailboxExecutor> consumerAndExecutor = Tuple2.of(null, null);


    public IterationChannel(IterationChannelKey channelKey) {
        this.channelKey = channelKey;
    }

    /**
     * Add Producer to this iteration Channel
     */
    public synchronized IterationQueue<T> addProducer(OperatorID operatorID) {
        Preconditions.checkState(!producers.containsKey(operatorID), "Duplicate Producers in queue");
        IterationQueue<T> queue = new IterationQueue<T>(consumerAndExecutor);
        producers.put(operatorID, queue);
        return queue;
    }

    /**
     * Set the consumer for this iteration Channel
     */
    public void setConsumer(Consumer<T> consumer, MailboxExecutor consumerExecutor) {
        Preconditions.checkState(consumerAndExecutor.f0 == null, "Cannot have 2 consumers for iteration channel");
        consumerAndExecutor.f0 = consumer;
        consumerAndExecutor.f1 = consumerExecutor;
    }

    /**
     * {@inheritDoc}
     * Should be only triggered by the Consumer Thread
     */
    @Override
    public void close() {
        producers.values().forEach(queue -> {
            IOUtils.closeQuietly(queue);
            queue.drain(val -> {
            }); // Drain all values
        });
        IterationChannelBroker.getBroker().removeChannel(channelKey);
    }

    /**
     * A wrapper Queue that the Producers directly interact with
     * Implements {@link Runnable} and directly passes itself to Consumer {@link Executor}
     * Implements {@link Closeable} to gracefully finish cleanup the iteration channel
     *
     * @param <T> Type of elements in this iteration
     */
    protected static class IterationQueue<T> extends SpscChunkedUnpaddedArrayQueue<T> implements ThrowingRunnable<Exception>, Closeable {

        /**
         * If this Runnable is still in {@link MailboxExecutor} do not schedule anymore since one run drains this queue
         */
        private final AtomicBoolean waiting = new AtomicBoolean(false);
        /**
         * Is this channel closed
         */
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Reference to the same field in the {@link IterationChannel}
         */
        @NotNull
        private final Tuple2<java.util.function.Consumer<T>, MailboxExecutor> consumerAndExecutor;

        public IterationQueue(@NotNull Tuple2<java.util.function.Consumer<T>, MailboxExecutor> consumerAndExecutor) {
            super(2 << 14); // 1 MB each array reference
            this.consumerAndExecutor = consumerAndExecutor;
        }

        /**
         * {@inheritDoc}
         * Added scheduling iteration scheduling logic
         */
        @Override
        public boolean add(T t) {
            if (closed.get()) return false;
            while (!super.offer(t)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // Try again
                }
            }
            if (!waiting.getAndSet(true)) scheduleMailbox();
            return true;
        }

        /**
         * Add one task to {@link MailboxExecutor}, and if closed also close the {@link IterationChannel}
         */
        public void scheduleMailbox() {
            try {
                consumerAndExecutor.f1.execute(this, "IterationMessage");
            } catch (NullPointerException | RejectedExecutionException ignored) {
                // Mailbox executor is closed can safely close this iteration channel, no new messages will be accepted
                System.out.println("Closing because" + ignored.getMessage());
                IOUtils.closeQuietly(this);
            }
        }

        /**
         * Starting iterating element from the startFlushing of this queue
         * Note that by entering this output HEAD can be closed but during the execution never
         */
        @Override
        public void run() {
            waiting.set(false);
            T el;
            while ((el = poll()) != null) {
                consumerAndExecutor.f0.accept(el);
            }
        }

        /**
         * {@inheritDoc}
         * Graceful finish if consumer thread executes this method
         * But can also happen in schedule or run functions as well
         */
        @Override
        public void close() throws IOException {
            closed.set(true);
        }

    }

}
