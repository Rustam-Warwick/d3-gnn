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

import operators.logger.FeedbackLogger;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/** Multi producer, single consumer channel. */
public final class FeedbackChannel<T> implements Closeable {

  /** The key that used to identify this channel. */
  private final SubtaskFeedbackKey<T> key;

  /**
   * Map from the iteration sink operators to their respective queues
   */
  public final ConcurrentHashMap<OperatorID, LockFreeBatchFeedbackQueue<T>> queues; // Producers in this channel

  /** A single registered consumer */
  private final AtomicReference<ConsumerTask<T>> consumerRef = new AtomicReference<>();

  FeedbackChannel(SubtaskFeedbackKey<T> key) {
    this.key = Objects.requireNonNull(key);
    this.queues = new ConcurrentHashMap<>();
  }

  /** Adds a feedback result to this channel. */
  public void put(T value, OperatorID publisherId) {
    if (!queues.get(publisherId).addAndCheckIfWasEmpty(value)) {
      // successfully added @value into the queue, but the queue wasn't (atomically) drained yet,
      // so there is nothing more to do.
      return;
    }
    @SuppressWarnings("resource")
    final ConsumerTask<T> consumer = consumerRef.get();
    if(Objects.nonNull(consumer)){
      consumer.scheduleDrainAll();
    }
  }

  /**
   * Registers one publisher with the given operatorId to this Channel
   * @param publisherId OperatorId of the published operator
   */
  public void registerPublisher(OperatorID publisherId){
    Preconditions.checkNotNull(publisherId);
    if(queues.containsKey(publisherId)){
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

    consumerTask.scheduleDrainAll();
  }

  /**
   * Registers snapshot in all queues
   */
  public void addSnapshotToAllQueues(long snapshotId){
    queues.values().forEach(item->item.addSnapshot(snapshotId));
  }

  /**
   * Adds snapshot to a specific queue
   */
  public void addSnapshotToQueue(long snapshotId, OperatorID operatorID){
    queues.get(operatorID).addSnapshot(snapshotId);
  }

  /**
   * Finished snapshots from all queues
   */
  public void finalizeSnapshotFromAllQueues(long snapshotId){
    queues.values().forEach(item->item.snapshotFinalize(snapshotId));
  }

  /**
   * Finish snapshot from a particular queue
   */
  public void finalizeSnapshotFromQueue(long snapshotId, OperatorID operatorID){
    queues.get(operatorID).snapshotFinalize(snapshotId);
  }

  /**
   * Synchronously log the state of the queue to the passed logger,
   * Done on snapshotting on iteration sink
   */
  public void dumpQueueToLogger(FeedbackLogger<T> logger, OperatorID operatorID) throws Exception {
    queues.get(operatorID).queue.logQueue(logger);
  }

  /** Closes this channel. */
  @Override
  public void close() {
    ConsumerTask<T> consumer = consumerRef.getAndSet(null);
    IOUtils.closeQuietly(consumer);
    // remove this channel.
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
        if(value.hasPendingSnapshots())continue; // Do not emit if valve has pending snapshots
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
    public void close() {}
  }
}
