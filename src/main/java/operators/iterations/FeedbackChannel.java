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

    private final ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues; // Multi-Producers

    private final SubtaskFeedbackKey<T> key; // Key of this FeedbackChannel

    private final AtomicReference<ConsumerTask<T>> consumerRef = new AtomicReference<>(); // Referenceof the ConsumerTask

    private final Phaser phaser = new Phaser(); // Phaser used for coordinating the finishing of the input

    FeedbackChannel(SubtaskFeedbackKey<T> key) {
        this.key = Objects.requireNonNull(key);
        this.queues = new ConcurrentHashMap<>();
    }

    public boolean hasConsumer() {
        return consumerRef.get() != null;
    }

    public boolean hasProducer() {
        return !queues.isEmpty();
    }

    /**
     * Adds a feedback result to this channel.
     */
    public void put(T value, OperatorID publisherId) {
        if(queues.containsKey(publisherId)){
            queues.get(publisherId).addAndCheckIfWasEmpty(value);
            @SuppressWarnings("resource") final ConsumerTask<T> consumer = consumerRef.get();
            if (Objects.nonNull(consumer)) {
                consumer.scheduleDrainAll();
            }
        }else{
            System.out.println("Such channel Does not exist");
        }
    }

    /**
     * Registers one publisher with the given operatorId to this Channel
     *
     * @param publisherId OperatorId of the published operator
     */
    public void registerPublisher(OperatorID publisherId) {
        Preconditions.checkNotNull(publisherId);
        if (queues.containsKey(publisherId)) {
            throw new IllegalStateException("There can be only a single producer with same operatorId in a FeedbackChannel.");
        }
        queues.putIfAbsent(publisherId, new LockFreeBatchFeedbackQueue<>());
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
        phaser.register();
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
     * @implNote Unsafe, use single threaded
     */
    public ArrayDeque<T> getUnsafeBuffer(OperatorID operatorID) {
        return queues.get(operatorID).queue.getUnsafeBuffer();
    }

    /**
     * Finish a specific producer
     */
    public void finishProducer(OperatorID operatorID) {
        if(queues.containsKey(operatorID)){
            queues.remove(operatorID); // Remove so that no new elements are accepted
            phaser.arrive();
        }
    }

    public Phaser getPhaser() {
        return phaser;
    }

    /**
     * Closes this channel. Use it from the consumer side
     */
    @Override
    public void close() {
        ConsumerTask<T> consumer = consumerRef.getAndSet(null);
        queues.values().clear();
        IOUtils.closeQuietly(consumer);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        broker.removeChannel(key);
    }

    private static final class ConsumerTask<T> implements Runnable, Closeable {
        private final Executor executor;
        private final FeedbackConsumer<T> consumer;
        private final ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues;

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
                        consumer.processFeedback(element);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void close() {
            queues.clear();
        }
    }
}
