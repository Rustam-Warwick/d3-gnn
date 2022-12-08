package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.netty4.io.netty.util.internal.shaded.org.jctools.queues.SpscLinkedQueue;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
    private volatile Tuple2<Consumer<T>, MailboxExecutor> consumerAndExecutor = null;

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
    public synchronized IterationQueue<T> addProducer(OperatorID operatorID){
        Preconditions.checkState(!producers.contains(operatorID), "Duplicate Producers in queue");
        IterationQueue<T> queue = new IterationQueue<T>(consumerAndExecutor);
        producers.put(operatorID, queue);
        return queue;
    }

    /**
     * Set the consumer for this iteration Channel
     */
    public synchronized void setConsumer(Consumer<T> consumer, MailboxExecutor consumerExecutor){
        Preconditions.checkState( consumerAndExecutor == null, "A IterationQueue cannot have multiple Consumers");
        this.consumerAndExecutor = Tuple2.of(consumer, consumerExecutor);
        producers.values().forEach(producer->producer.setLateConsumerAndExecutor(this.consumerAndExecutor));
    }

    /**
     * {@inheritDoc}
     * Should be only triggered by the Consumer Thread
     */
    @Override
    public void close() {
        producers.values().forEach(IOUtils::closeQuietly);
        IterationChannelBroker.getBroker().removeChannel(channelKey);
    }

    /**
     * A wrapper Queue that the Producers directly interact with
     * Implements {@link Runnable} and directly passes itself to Consumer {@link Executor}
     * Implements {@link Closeable} to gracefully finish cleanup the iteration channel
     * @param <T> Type of elements in this iteration
     */
    protected static class IterationQueue<T> extends SpscLinkedQueue<T> implements ThrowingRunnable<Exception>, Closeable {

        /**
         * Reference to the same field in the {@link IterationChannel}
         */
        @Nullable
        private volatile Tuple2<java.util.function.Consumer<T>, MailboxExecutor> consumerAndExecutor;

        /**
         * If this Runnable is still in {@link MailboxExecutor} do not schedule anymore
         */
        private final AtomicBoolean waiting = new AtomicBoolean(false);

        public IterationQueue(@Nullable Tuple2<java.util.function.Consumer<T>, MailboxExecutor> consumerAndExecutor) {
            this.consumerAndExecutor = consumerAndExecutor;
        }

        /**
         * Set the late Consumer and Executor and do the first submit to {@link MailboxExecutor}
         * Latter is needed cause there might be messages added to this queue before Consumer registering
         */
        public void setLateConsumerAndExecutor(@NotNull Tuple2<java.util.function.Consumer<T>, MailboxExecutor> consumerAndExecutor) {
            if(!isEmpty() && !waiting.getAndSet(true)){
                this.consumerAndExecutor = consumerAndExecutor;
                this.consumerAndExecutor.f1.execute(this, "IterationMessage");
            }else{
                this.consumerAndExecutor = consumerAndExecutor;
            }
        }

        /**
         * {@inheritDoc}
         * Added scheduling iteration scheduling logic
         */
        @Override
        public boolean add(T t) {
            boolean res = super.add(t);
            try{
                if(consumerAndExecutor != null && !waiting.getAndSet(true)){
                    consumerAndExecutor.f1.execute(this, "IterationMessage");
                }
            }catch (NullPointerException | RejectedExecutionException ignored){
                // Pass: This means that the mailbox is already closed or draining. So we can close prematurely
                IOUtils.closeQuietly(this);
            }
            return res;
        }

        /**
         * Starting iterating element from the start of this queue
         * Note that by entering this message HEAD can be closed but during the execution never
         */
        @Override
        public void run() {
            if(consumerAndExecutor == null) return; // Can happen that some messages are stuck after mailbox closing
            waiting.set(false);
            T el;
            while((el = poll()) != null){
                consumerAndExecutor.f0.accept(el);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            consumerAndExecutor = null;
        }

    }

}
