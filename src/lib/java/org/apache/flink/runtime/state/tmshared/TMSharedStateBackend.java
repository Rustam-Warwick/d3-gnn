package org.apache.flink.runtime.state.tmshared;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;

/**
 * Special State Backend supporting In-Memory shared state within single Task Manager
 * <p>
 * Wraps around internal {@link StateBackend} hence can be used with RockDB or HashMapStateBackend
 * Nonetheless, shared state will be stored in memory.
 * @todo Implement TMOperatorStateBackend for non-keyed streams
 * @todo Add proper fault-tolerance for such state backends
 * </p>
 */
public class TMSharedStateBackend extends AbstractStateBackend {

    /**
     * Original (wrapped) {@link AbstractStateBackend}
     */
    protected final AbstractStateBackend wrappedBackend;

    private TMSharedStateBackend(AbstractStateBackend wrappedBackend) {
        this.wrappedBackend = wrappedBackend;
    }

    /**
     * Factory method for creating such state backends
     */
    public static TMSharedStateBackend with(AbstractStateBackend wrappedBackend) {
        return new TMSharedStateBackend(wrappedBackend);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @NotNull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws IOException {
        AbstractKeyedStateBackend<K> wrappedKeyedStateBackend = wrappedBackend.createKeyedStateBackend(env, jobID, operatorIdentifier, keySerializer, numberOfKeyGroups, keyGroupRange, kvStateRegistry, ttlTimeProvider, metricGroup, stateHandles, cancelStreamRegistry);
        return new TMSharedKeyedStateBackendBuilder<>(
                kvStateRegistry,
                keySerializer,
                env.getUserCodeClassLoader().asClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                env.getExecutionConfig(),
                ttlTimeProvider,
                wrappedKeyedStateBackend.getLatencyTrackingStateConfig(),
                stateHandles,
                wrappedKeyedStateBackend.getKeyGroupCompressionDecorator(),
                cancelStreamRegistry,
                wrappedKeyedStateBackend,
                env
        ).build();
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @NotNull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry, double managedMemoryFraction) throws Exception {
        AbstractKeyedStateBackend<K> wrappedKeyedStateBackend = (AbstractKeyedStateBackend<K>) wrappedBackend.createKeyedStateBackend(env, jobID, operatorIdentifier, keySerializer, numberOfKeyGroups, keyGroupRange, kvStateRegistry, ttlTimeProvider, metricGroup, stateHandles, cancelStreamRegistry, managedMemoryFraction);
        return new TMSharedKeyedStateBackendBuilder<>(
                kvStateRegistry,
                keySerializer,
                env.getUserCodeClassLoader().asClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                env.getExecutionConfig(),
                ttlTimeProvider,
                wrappedKeyedStateBackend.getLatencyTrackingStateConfig(),
                stateHandles,
                wrappedKeyedStateBackend.getKeyGroupCompressionDecorator(),
                cancelStreamRegistry,
                wrappedKeyedStateBackend,
                env
        ).build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @NotNull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
        return wrappedBackend.createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    @Override
    public String getName() {
        return "Graph +" + wrappedBackend.getName();
    }

    @Override
    public boolean useManagedMemory() {
        return wrappedBackend.useManagedMemory();
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        return wrappedBackend.supportsNoClaimRestoreMode();
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return wrappedBackend.supportsSavepointFormat(formatType);
    }
}
