/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package operators.iterations;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.util.IOUtils;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi producer, single consumer channel.
 */
public final class FeedbackChannel<T> implements Closeable {

    private final ConcurrentHashMap<OperatorID, Tuple3<LockFreeBatchFeedbackQueue<T>, ConsumerTask<T>, Tuple2<Meter, Meter>>> publishers = new ConcurrentHashMap<>(); // Multi-Producers
    private final SubtaskFeedbackKey<T> key; // Key of this FeedbackChannel
    private final Phaser phaser = new Phaser(); // Phaser used for coordinating the finishing of the input
    private Tuple2<FeedbackConsumer<T>, Executor> consumer; // Consumer

    FeedbackChannel(SubtaskFeedbackKey<T> key) {
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Has this channel any consumers
     */
    public boolean hasConsumer() {
        return consumer != null;
    }

    /**
     * Has this channel any Producers
     */
    public boolean hasProducer() {
        return !publishers.isEmpty();
    }

    /**
     * Adds a feedback result to this channel.
     */
    public void put(T value, OperatorID publisherId) {
        publishers.computeIfPresent(publisherId, (key, val) -> {
            val.f0.addAndCheckIfWasEmpty(value);
            if (val.f1 != null) val.f1.schedule();
            return val;
        });
    }

    /**
     * Registers one publisher with the given operatorId to this Channel
     *
     * @param publisherId OperatorId of the published operator
     */
    public void registerPublisher(OperatorID publisherId, Tuple2<Meter, Meter> flowCounters) {
        synchronized (this) {
            assert !publishers.containsKey(publisherId);
            publishers.computeIfAbsent(publisherId, (key) -> {
                LockFreeBatchFeedbackQueue<T> tmp = new LockFreeBatchFeedbackQueue<>();
                if (consumer == null) return Tuple3.of(tmp, null, flowCounters);
                else return Tuple3.of(tmp, new ConsumerTask<>(consumer.f1, consumer.f0, tmp), flowCounters);
            });
        }
    }

    /**
     * Register a feedback iteration consumer
     *
     * @param consumer the feedback events consumer.
     * @param executor the executor to schedule feedback consumption on.
     */
    public void registerConsumer(final FeedbackConsumer<T> consumer, Executor executor) {
        synchronized (this) {
            assert this.consumer == null;
            this.consumer = Tuple2.of(consumer, executor);
            publishers.forEach((key, value) -> {
                value.f1 = new ConsumerTask<>(executor, consumer, value.f0);
            });
        }
    }

    /**
     * Registers snapshot in all queues
     */
    public void addSnapshot(long snapshotId) {
        publishers.forEach((key, item) -> {
            item.f0.addSnapshot(snapshotId);
        });
    }

    /**
     * Finish snapshot from a particular queue
     */
    public void finishSnapshot(long snapshotId, OperatorID operatorID) {
        publishers.get(operatorID).f0.snapshotFinalize(snapshotId);
    }

    /**
     * Get the buffer of the queue without the lockm only use if the buffer is not being modified
     *
     * @implNote Unsafe, use single threaded
     */
    public ArrayDeque<T> getUnsafeBuffer(OperatorID operatorID) {
        return publishers.get(operatorID).f0.queue.getUnsafeBuffer();
    }

    /**
     * Phase used to coordinate the closing of this iteration step
     */
    public Phaser getPhaser() {
        return phaser;
    }

    /**
     * Get total messages being sent from the <strong>Operator</strong> of the iteration producers
     * Used in the iteration consumer to detect termination
     */
    public double getTotalFlowingMessageCount() {
        return publishers.values().stream().mapToDouble(item -> item.f2.f0.getRate() + item.f2.f1.getRate()).sum();
    }

    /**
     * Closes this channel. Use it from the consumer side
     */
    @Override
    public void close() {
        publishers.forEach((opId, val) -> IOUtils.closeQuietly(val.f1));
        publishers.clear();
        phaser.forceTermination();
        FeedbackChannelBroker.get().removeChannel(key);
    }

    /**
     * Actual task that is run on each IterationSource MailboxExecutor
     *
     * @param <T>
     */
    private static final class ConsumerTask<T> implements Runnable, Closeable {
        private final Executor executor;

        private final LockFreeBatchFeedbackQueue<T> queue;

        private final AtomicInteger scheduleCount = new AtomicInteger();

        private FeedbackConsumer<T> consumer;

        ConsumerTask(Executor executor, FeedbackConsumer<T> consumer, LockFreeBatchFeedbackQueue<T> queue) {
            this.executor = Objects.requireNonNull(executor);
            this.consumer = Objects.requireNonNull(consumer);
            this.queue = Objects.requireNonNull(queue);
        }

        void schedule() {
            if (scheduleCount.get() == 0 && consumer != null) {
                scheduleCount.incrementAndGet();
                executor.execute(this);
            }
        }

        @Override
        public void run() {
            scheduleCount.decrementAndGet();
            if (queue.hasPendingSnapshots() || consumer == null) return;
            final Deque<T> buffer = queue.drainAll();
            try {
                T element;
                while ((element = buffer.pollFirst()) != null) {
                    consumer.processFeedback(element);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * {@inheritDoc}
         * run() and close() cannot be run in parallel
         */
        @Override
        public void close() {
            consumer = null;
        }
    }
}
