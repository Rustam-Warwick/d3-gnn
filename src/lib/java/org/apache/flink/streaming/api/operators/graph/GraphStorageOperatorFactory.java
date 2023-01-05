package org.apache.flink.streaming.api.operators.graph;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.util.Preconditions;
import storage.BaseStorage;
import storage.DefaultStorage;

import java.util.List;

/**
 * {@link AbstractStreamOperatorFactory} for {@link GraphStorageOperator}
 */
public class GraphStorageOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements CoordinatedOperatorFactory<GraphOp>, OneInputStreamOperatorFactory<GraphOp, GraphOp> {

    /**
     * List of {@link Plugin} working for this storage operator
     */
    final protected List<Plugin> plugins;

    /**
     * Position of this storage operator in the entire pipeline [1..layers]
     */
    final protected short position;

    /**
     * Class of storage to be used for storing elements
     */
    final protected Class<? extends BaseStorage> storageClass;


    public GraphStorageOperatorFactory(List<Plugin> plugins, short position, Class<? extends BaseStorage> storageClass){
        Preconditions.checkNotNull(plugins, "Plugins cannot be null");
        Preconditions.checkState(plugins.stream().allMatch(plugin -> plugin.getId() != null), "Plugin ID should be non-null and unique");
        this.position = position;
        this.plugins = plugins;
        this.storageClass = storageClass;
    }

    public GraphStorageOperatorFactory(List<Plugin> plugins, short position) {
        Preconditions.checkNotNull(plugins, "Plugins cannot be null");
        Preconditions.checkState(plugins.stream().allMatch(plugin -> plugin.getId() != null), "Plugin ID should be non-null and unique");
        this.position = position;
        this.plugins = plugins;
        this.storageClass = DefaultStorage.class;
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        return (T) new GraphStorageOperator(plugins, position, ConstructorAccess.get(storageClass), parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return GraphStorageOperator.class;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new GraphOperatorCoordinator.GraphOperatorCoordinatorProvider(position, operatorID);
    }
}
