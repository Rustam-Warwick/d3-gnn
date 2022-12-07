package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.netty4.io.netty.util.internal.shaded.org.jctools.queues.SpscLinkedQueue;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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
    private final ConcurrentHashMap<OperatorID, SpscLinkedQueue<T>> producers = new ConcurrentHashMap<>(5);

    /**
     * Consumer of this channel
     */
    private final Tuple2<Consumer<T>, Executor> consumerAndExecutor = Tuple2.of(null, null);

    /**
     * ID of this channel
     */
    private final Tuple3<JobID, Integer, Integer> id;

    public IterationChannel(Tuple3<JobID, Integer, Integer> id) {
        this.id = id;
    }

    /**
     * Add Producer to this iteration Channel
     */
    public void addProducer(OperatorID operatorID){
        Preconditions.checkState(!producers.contains(operatorID));
        producers.put(operatorID, new SpscLinkedQueue<>());
    }

    /**
     * Set the consumer for this iteration Channel
     */
    public void setConsumer(Consumer<T> consumer, Executor consumerExecutor){
        this.consumerAndExecutor.f0 = consumer;
        this.consumerAndExecutor.f1 = consumerExecutor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }
}
