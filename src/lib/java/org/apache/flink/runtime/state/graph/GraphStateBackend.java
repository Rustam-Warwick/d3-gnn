package org.apache.flink.runtime.state.graph;

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
 * Special State Backend supporting In-Memory Graphs
 * <p>
 *     Wraps around internal {@link StateBackend} hence can be used with RockDB or HashMapStateBackend
 *     Nonetheless, Graph will be stored in memory
 * </p>
 */
public class GraphStateBackend extends AbstractStateBackend {

    /**
     * Backend that is wrapped with this
     */
    protected final AbstractStateBackend wrappedBackend;

    /**
     * Base Graph class
     */
    protected final Class<? extends BaseGraphState> baseGraphClass;

    private GraphStateBackend(AbstractStateBackend wrappedBackend, Class<? extends BaseGraphState> baseGraphClass) {
        this.wrappedBackend = wrappedBackend;
        this.baseGraphClass = baseGraphClass;
    }

    public static GraphStateBackend with(AbstractStateBackend wrappedBackend, Class<? extends BaseGraphState> baseGraphClass){
        return new GraphStateBackend(wrappedBackend, baseGraphClass);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @NotNull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws IOException {
        AbstractKeyedStateBackend<K> wrappedKeyedStateBackend = wrappedBackend.createKeyedStateBackend(env, jobID, operatorIdentifier, keySerializer, numberOfKeyGroups, keyGroupRange, kvStateRegistry, ttlTimeProvider, metricGroup, stateHandles, cancelStreamRegistry);
        return new GraphKeyedStateBackendBuilder<>(
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
                baseGraphClass,
                env
        ).build();
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @NotNull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry, double managedMemoryFraction) throws Exception {
        AbstractKeyedStateBackend<K> wrappedKeyedStateBackend = (AbstractKeyedStateBackend<K>) wrappedBackend.createKeyedStateBackend(env, jobID, operatorIdentifier, keySerializer, numberOfKeyGroups, keyGroupRange, kvStateRegistry, ttlTimeProvider, metricGroup, stateHandles, cancelStreamRegistry, managedMemoryFraction);
        return new GraphKeyedStateBackendBuilder<>(
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
                baseGraphClass,
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
