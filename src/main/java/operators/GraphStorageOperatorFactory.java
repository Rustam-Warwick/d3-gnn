package operators;

import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.util.Preconditions;
import storage.BaseStorage;

import java.util.List;

public class GraphStorageOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements OneInputStreamOperatorFactory<GraphOp, GraphOp> {

    final protected List<Plugin> plugins;

    final protected BaseStorage storage;

    public GraphStorageOperatorFactory(List<Plugin> plugins, BaseStorage storage) {
        Preconditions.checkNotNull(plugins, "Plugins cannot be null");
        Preconditions.checkNotNull(storage, "Storage cannot be null");
        Preconditions.checkState(plugins.stream().allMatch(plugin -> plugin.getId() != null), "Plugin ID should be non-null and unique");
        this.plugins = plugins;
        this.storage = storage;
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        return (T) new GraphStorageOperator(plugins, storage, parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return GraphStorageOperator.class;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }
}
