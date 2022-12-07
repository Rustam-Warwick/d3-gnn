package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.netty4.io.netty.util.internal.shaded.org.jctools.queues.SpscLinkedQueue;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * <p>
 *     This is the main in-memory channel that follows MPSC approach
 *     MPSC Queue is implemented by maintaining multiple SPSC queues, one per producer
 * </p>
 * @param <T> Type of elements iterating in this channel
 */
public class IterationChannel<T> implements Closeable {

    /**
     * List of producers identified by their IDs
     */
    private final ConcurrentHashMap<OperatorID, IterationQueue<T>> producers = new ConcurrentHashMap<>(5);

    /**
     * Consumer & Executor of this channel, changes are made entirely for volatile to take effect
     */
    private volatile Tuple2<ThrowingConsumer<T, Exception>, MailboxExecutor> consumerAndExecutor = Tuple2.of(null, null);

    /**
     * ID of this channel
     */
    private final IterationChannelKey channelKey;

    public IterationChannel(IterationChannelKey channelKey) {
        this.channelKey = channelKey;
    }

    /**
     * Add Producer to this iteration Channel
     */
    public IterationQueue<T> addProducer(OperatorID operatorID){
        Preconditions.checkState(!producers.contains(operatorID));
        IterationQueue<T> queue = new IterationQueue<T>(consumerAndExecutor);
        producers.put(operatorID, queue);
        return queue;
    }

    /**
     * Set the consumer for this iteration Channel
     */
    public void setConsumer(ThrowingConsumer<T, Exception> consumer, MailboxExecutor consumerExecutor){
        Preconditions.checkState(consumerAndExecutor.f1 == null && consumerAndExecutor.f0 == null, "A IterationQueue cannot have multiple Consumers");
        this.consumerAndExecutor = Tuple2.of(consumer, consumerExecutor);
        producers.values().forEach(producer->producer.setConsumerAndExecutor(this.consumerAndExecutor));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        IterationChannelBroker.getBroker().removeChannel(channelKey);
    }

    /**
     * A wrapper Queue that the Producers directly interact with
     * Implements {@link Runnable} and directly passes itself to Consumer {@link Executor}
     * Implements {@link Closeable} to gracefully finish cleanup the iteration channel
     * @param <T> Type of elements in this iteration
     */
    protected static class IterationQueue<T> extends SpscLinkedQueue<T> implements Runnable, Closeable {

        /**
         * Reference to the same field in the {@link IterationChannel}
         */
        private volatile Tuple2<ThrowingConsumer<T, Exception>, MailboxExecutor> consumerAndExecutor;

        public IterationQueue(Tuple2<ThrowingConsumer<T, Exception>, MailboxExecutor> consumerAndExecutor) {
            this.consumerAndExecutor = consumerAndExecutor;
        }

        public void setConsumerAndExecutor(Tuple2<ThrowingConsumer<T, Exception>, MailboxExecutor> consumerAndExecutor) {
            this.consumerAndExecutor = consumerAndExecutor;
        }

        /**
         * Starting iterating element from the start of this queue
         */
        @Override
        public void run() {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {

        }

    }

}
