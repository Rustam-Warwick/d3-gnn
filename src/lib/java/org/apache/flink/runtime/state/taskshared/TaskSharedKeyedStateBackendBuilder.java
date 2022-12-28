package org.apache.flink.runtime.state.taskshared;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Builder pattern for {@link TaskSharedKeyedStateBackend}
 * @param <K>
 */
public class TaskSharedKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {

    protected final AbstractKeyedStateBackend<K> wrappedKeyedStateBackend;

    protected final Environment environment;


    public TaskSharedKeyedStateBackendBuilder(TaskKvStateRegistry kvStateRegistry,
                                              TypeSerializer<K> keySerializer,
                                              ClassLoader userCodeClassLoader,
                                              int numberOfKeyGroups,
                                              KeyGroupRange keyGroupRange,
                                              ExecutionConfig executionConfig,
                                              TtlTimeProvider ttlTimeProvider,
                                              LatencyTrackingStateConfig latencyTrackingStateConfig,
                                              @NotNull Collection<KeyedStateHandle> stateHandles,
                                              StreamCompressionDecorator keyGroupCompressionDecorator,
                                              CloseableRegistry cancelStreamRegistry,
                                              AbstractKeyedStateBackend<K> wrappedKeyedStateBackend,
                                              Environment environment) {
        super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider, latencyTrackingStateConfig, stateHandles, keyGroupCompressionDecorator, cancelStreamRegistry);
        this.wrappedKeyedStateBackend = wrappedKeyedStateBackend;
        this.environment = environment;
    }

    @Override
    public AbstractKeyedStateBackend<K> build() throws BackendBuildingException {
        return new TaskSharedKeyedStateBackend<>(
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                wrappedKeyedStateBackend.getKeyContext(),
                wrappedKeyedStateBackend,
                Tuple2.of(environment.getJobID(), environment.getJobVertexId())
        );
    }
}
