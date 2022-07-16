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
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Multi producer, single consumer channel.
 */
public final class FeedbackChannel<T> implements Closeable {

    public final ConcurrentHashMap<OperatorID, Tuple2<Meter, Meter>> meters; // Multi-Producer meters

    private final ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues; // Multi-Producers

    private final SubtaskFeedbackKey<T> key; // Key of this FeedbackChannel

    private final AtomicReference<ConsumerTask<T>> consumerRef = new AtomicReference<>(); // Referenceof the ConsumerTask

    private final Phaser phaser = new Phaser(); // Phaser used for coordinating the finishing of the input

    FeedbackChannel(SubtaskFeedbackKey<T> key) {
        this.key = Objects.requireNonNull(key);
        this.queues = new ConcurrentHashMap<>();
        this.meters = new ConcurrentHashMap<>();
    }

    /**
     * Has this channel any consumers
     */
    public boolean hasConsumer() {
        return consumerRef.get() != null;
    }

    /**
     * Has this channel any Producers
     */
    public boolean hasProducer() {
        return !queues.isEmpty();
    }

    /**
     * Adds a feedback result to this channel.
     */
    public void put(T value, OperatorID publisherId) {
        if (queues.containsKey(publisherId)) {
            queues.get(publisherId).addAndCheckIfWasEmpty(value);
            @SuppressWarnings("resource") final ConsumerTask<T> consumer = consumerRef.get();
            if (Objects.nonNull(consumer)) {
                consumer.scheduleDrainAll();
            }
        } else {
            // Can only happen if the channel is interrupted, external job cancel signal
            // Not an issue since it is closing any way
        }
    }

    /**
     * Registers one publisher with the given operatorId to this Channel
     *
     * @param publisherId OperatorId of the published operator
     */
    public void registerPublisher(OperatorID publisherId, Tuple2<Meter, Meter> numRecordsInCounter) {
        Preconditions.checkNotNull(publisherId);
        if (queues.containsKey(publisherId)) {
            throw new IllegalStateException("There can be only a single producer with same operatorId in a FeedbackChannel.");
        }
        queues.putIfAbsent(publisherId, new LockFreeBatchFeedbackQueue<>());
        meters.putIfAbsent(publisherId, numRecordsInCounter);
    }

    /**
     * Register a feedback iteration consumer
     *
     * @param consumer the feedback events consumer.
     * @param executor the executor to schedule feedback consumption on.
     */
    public void registerConsumer(final FeedbackConsumer<T> consumer, Executor executor) {
        Preconditions.checkNotNull(consumer);
        ConsumerTask<T> consumerTask = new ConsumerTask<T>(executor, consumer, queues);
        if (!this.consumerRef.compareAndSet(null, consumerTask)) {
            throw new IllegalStateException("There can be only a single consumer in a FeedbackChannel.");
        }
    }

    /**
     * Registers snapshot in all queues
     */
    public void addSnapshot(long snapshotId) {
        queues.forEach((key, item) -> {
            item.addSnapshot(snapshotId);
        });
    }

    /**
     * Finish snapshot from a particular queue
     */
    public void finishSnapshot(long snapshotId, OperatorID operatorID) {
        queues.get(operatorID).snapshotFinalize(snapshotId);
    }

    /**
     * Get the buffer of the queue without the lockm only use if the buffer is not being modified
     *
     * @implNote Unsafe, use single threaded
     */
    public ArrayDeque<T> getUnsafeBuffer(OperatorID operatorID) {
        return queues.get(operatorID).queue.getUnsafeBuffer();
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
    public double getTotalFlowingMessagesRate() {
        return meters.values().stream().mapToDouble(item -> item.f0.getRate() + item.f1.getRate()).sum();
    }

    /**
     * Closes this channel. Use it from the consumer side
     */
    @Override
    public void close() {
        queues.clear();
        meters.clear();
        phaser.forceTermination();
        ConsumerTask<T> consumer = consumerRef.getAndSet(null);
        IOUtils.closeQuietly(consumer);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        broker.removeChannel(key);
    }

    /**
     * Actual task that is run on each IterationSource MailboxExecutor
     *
     * @param <T>
     */
    private static final class ConsumerTask<T> implements Runnable, Closeable {
        private final Executor executor;
        private final ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues;
        private FeedbackConsumer<T> consumer;

        ConsumerTask(Executor executor, FeedbackConsumer<T> consumer, ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues) {
            this.executor = Objects.requireNonNull(executor);
            this.consumer = Objects.requireNonNull(consumer);
            this.queues = Objects.requireNonNull(queues);
        }

        void scheduleDrainAll() {
            executor.execute(this);
        }

        @Override
        public void run() {
            for (LockFreeBatchFeedbackQueue<T> value : queues.values()) {
                if (value.hasPendingSnapshots()) {
                    continue;
                }
                final Deque<T> buffer = value.drainAll();
                try {
                    T element;
                    while ((element = buffer.pollFirst()) != null) {
                        if (consumer == null) return;
                        consumer.processFeedback(element);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void close() {
            consumer = null;
        }
    }
}
