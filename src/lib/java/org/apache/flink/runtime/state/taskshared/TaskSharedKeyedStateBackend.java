package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.NotNull;
import storage.BaseStorage;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * Keyed State Backend for {@link TaskSharedStateBackend}
 * <p>
 *     As with {@link TaskSharedStateBackend} it wraps around a pre-defined {@link KeyedStateBackend} and
 *     basically uses it for all normal state operations
 *     On top of it it implements methods for handling task local state access
 * </p>
 * @param <K> Type of Keys
 */
public class TaskSharedKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    /**
     * Map table of the {@link BaseStorage} per task
     * Key is [JobID, JobVertexID, State Name, NameSpace]. NameSpace for task shared state cannot be changed afterwards
     */
    final protected static Map<Tuple4<JobID, JobVertexID, String, Object>, TaskSharedState> TASK_LOCAL_STATE_MAP = new NonBlockingHashMap<>();

    /**
     * Wrapped delegate state backend
     */
    final protected AbstractKeyedStateBackend<K> wrappedKeyedStateBackend;

    /**
     * Identifier to this task within all local operators
     */
    final protected Tuple2<JobID, JobVertexID> taskIdentifier;


    public TaskSharedKeyedStateBackend(TaskKvStateRegistry kvStateRegistry,
                                       TypeSerializer<K> keySerializer,
                                       ClassLoader userCodeClassLoader,
                                       ExecutionConfig executionConfig,
                                       TtlTimeProvider ttlTimeProvider,
                                       LatencyTrackingStateConfig latencyTrackingStateConfig,
                                       CloseableRegistry cancelStreamRegistry,
                                       InternalKeyContext<K> keyContext,
                                       AbstractKeyedStateBackend<K> wrappedKeyedStateBackend,
                                       Tuple2<JobID, JobVertexID> taskIdentifier) {
        super(kvStateRegistry, keySerializer, userCodeClassLoader, executionConfig, ttlTimeProvider, latencyTrackingStateConfig, cancelStreamRegistry, keyContext);
        this.wrappedKeyedStateBackend = wrappedKeyedStateBackend;
        this.taskIdentifier = taskIdentifier;
    }

    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        wrappedKeyedStateBackend.notifyCheckpointSubsumed(checkpointId);
    }

    @Override
    public void dispose() {
        wrappedKeyedStateBackend.dispose();
    }

    @Override
    public void setCurrentKey(K newKey) {
        wrappedKeyedStateBackend.setCurrentKey(newKey);
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        wrappedKeyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return wrappedKeyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return wrappedKeyedStateBackend.getKeySerializer();
    }

    @Override
    public K getCurrentKey() {
        return wrappedKeyedStateBackend.getCurrentKey();
    }

    public int getCurrentKeyGroupIndex() {
        return wrappedKeyedStateBackend.getCurrentKeyGroupIndex();
    }

    public int getNumberOfKeyGroups() {
        return wrappedKeyedStateBackend.getNumberOfKeyGroups();
    }

    public KeyGroupRange getKeyGroupRange() {
        return wrappedKeyedStateBackend.getKeyGroupRange();
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor, KeyedStateFunction<K, S> function) throws Exception {
        wrappedKeyedStateBackend.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
    }

    public <N, S extends State, T> void applyToAllKeys(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor, KeyedStateFunction<K, S> function, AbstractKeyedStateBackend.PartitionStateFactory partitionStateFactory) throws Exception {
        wrappedKeyedStateBackend.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function, partitionStateFactory);
    }

    @Override
    public <N, S extends State, V> S getOrCreateKeyedState(TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor) throws Exception {
        return wrappedKeyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
    }
    @Override
    public void publishQueryableStateIfEnabled(StateDescriptor<?, ?> stateDescriptor, InternalKvState<?, ?, ?> kvState) {
        wrappedKeyedStateBackend.publishQueryableStateIfEnabled(stateDescriptor, kvState);
    }

    /**
     *  Create or get {@link TaskSharedState} from backend
     */
    public <N, S extends TaskSharedState> S getOrCreateTaskSharedState(N namespace, TypeSerializer<N> nameSpaceSerializer, TaskSharedStateDescriptor<S, ?> taskSharedStateDescriptor){
        TaskSharedState taskLocal = TASK_LOCAL_STATE_MAP.compute(Tuple4.of(taskIdentifier.f0, taskIdentifier.f1, taskSharedStateDescriptor.getName(), namespace), (key, val)->(
           val == null ? taskSharedStateDescriptor.getStateSupplier().get():val
        ));
        taskLocal.register(this);
        return (S) taskLocal;
    }

    @Override
    public <N, S extends State> S getPartitionedState(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
        return wrappedKeyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
    }

    public void close() throws IOException {
        wrappedKeyedStateBackend.close();
    }

    public LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        return wrappedKeyedStateBackend.getLatencyTrackingStateConfig();
    }

    @VisibleForTesting
    public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
        return wrappedKeyedStateBackend.getKeyGroupCompressionDecorator();
    }

    @VisibleForTesting
    public int numKeyValueStatesByName() {
        return wrappedKeyedStateBackend.numKeyValueStatesByName();
    }

    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return wrappedKeyedStateBackend.requiresLegacySynchronousTimerSnapshots(checkpointType);
    }

    public InternalKeyContext<K> getKeyContext() {
        return wrappedKeyedStateBackend.getKeyContext();
    }

    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        wrappedKeyedStateBackend.setCurrentKeyGroupIndex(currentKeyGroupIndex);
    }

    @Nonnull
    public SavepointResources<K> savepoint() throws Exception {
        return wrappedKeyedStateBackend.savepoint();
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return wrappedKeyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return wrappedKeyedStateBackend.getKeysAndNamespaces(state);
    }

    @Override
    @Deprecated
    public boolean isStateImmutableInStateBackend(CheckpointType checkpointOptions) {
        return wrappedKeyedStateBackend.isStateImmutableInStateBackend(checkpointOptions);
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return wrappedKeyedStateBackend.isSafeToReuseKVState();
    }

    @Override
    @Nonnull
    public <N, SV, S extends State, IS extends S> IS createOrUpdateInternalState(@NotNull TypeSerializer<N> namespaceSerializer, @NotNull StateDescriptor<S, SV> stateDesc) throws Exception {
        return wrappedKeyedStateBackend.createOrUpdateInternalState(namespaceSerializer, stateDesc);
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(@NotNull TypeSerializer<N> namespaceSerializer, @NotNull StateDescriptor<S, SV> stateDesc, @NotNull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
        return wrappedKeyedStateBackend.createOrUpdateInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory);
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(@NotNull TypeSerializer<N> namespaceSerializer, @NotNull StateDescriptor<S, SV> stateDesc, @NotNull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory, boolean allowFutureMetadataUpdates) throws Exception {
        return wrappedKeyedStateBackend.createOrUpdateInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory, allowFutureMetadataUpdates);
    }

    @Override
    @Nonnull
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(@NotNull String stateName, @NotNull TypeSerializer<T> byteOrderedElementSerializer) {
        return wrappedKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(@NotNull String stateName, @NotNull TypeSerializer<T> byteOrderedElementSerializer, boolean allowFutureMetadataUpdates) {
        return wrappedKeyedStateBackend.create(stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @Nonnull
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(long checkpointId, long timestamp, @NotNull CheckpointStreamFactory streamFactory, @NotNull CheckpointOptions checkpointOptions) throws Exception {
        return wrappedKeyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedKeyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrappedKeyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    public int numKeyValueStateEntries() {
        return wrappedKeyedStateBackend.numKeyValueStateEntries();
    }

    public KeyedStateBackend<K> getDelegatedKeyedStateBackend(boolean recursive) {
        return wrappedKeyedStateBackend.getDelegatedKeyedStateBackend(recursive);
    }
}
