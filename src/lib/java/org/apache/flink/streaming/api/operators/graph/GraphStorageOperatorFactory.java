package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.util.Preconditions;
import storage.GraphStorageProvider;

import java.util.List;

/**
 * {@link AbstractStreamOperatorFactory} for {@link GraphStorageOperator}
 * Has {@link GraphOperatorCoordinator}
 */
public class GraphStorageOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements CoordinatedOperatorFactory<GraphOp>, OneInputStreamOperatorFactory<GraphOp, GraphOp>, ProcessingTimeServiceAware {

    /**
     * List of {@link Plugin} working for this storage operator
     */
    final protected List<Plugin> plugins;

    /**
     * Position of this storage operator in the entire pipeline [1..layers]
     */
    final protected short position;

    /**
     * Number of layers in the pipeline
     */
    final protected short layers;

    /**
     * Class of storage to be used for storing elements
     */
    final protected GraphStorageProvider storageProvider;

    /**
     * Supplier for {@link GraphOperatorCoordinator.GraphOperatorSubCoordinator}
     */
    final protected GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider;

    /**
     * Processing Time service to be passed to operator
     */
    transient protected ProcessingTimeService processingTimeService;


    public GraphStorageOperatorFactory(List<Plugin> plugins, short position, short layers, GraphStorageProvider storageProvider, GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
        Preconditions.checkState(position > 0, "Position should be greated than 0, 0 is for Splitter operator");
        Preconditions.checkNotNull(plugins, "Plugins cannot be null");
        Preconditions.checkState(plugins.stream().allMatch(plugin -> plugin.getId() != null), "Plugin ID should be non-null and unique");
        this.position = position;
        this.layers = layers;
        this.plugins = plugins;
        this.storageProvider = storageProvider;
        this.graphOperatorSubCoordinatorsProvider = graphOperatorSubCoordinatorsProvider;
    }

    public GraphStorageOperatorFactory(List<Plugin> plugins, short position, short layers, GraphStorageProvider storageProvider) {
        this(plugins, position, layers, storageProvider, new GraphOperatorCoordinator.DefaultGraphOperatorSubCoordinatorsProvider());
    }

    public GraphStorageOperatorFactory(List<Plugin> plugins, short position, short layers, GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
        this(plugins, position, layers, new GraphStorageProvider.DefaultGraphStorageProvider(), graphOperatorSubCoordinatorsProvider);
    }

    public GraphStorageOperatorFactory(List<Plugin> plugins, short position, short layers) {
        this(plugins, position, layers, new GraphStorageProvider.DefaultGraphStorageProvider());
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        return (T) new GraphStorageOperator(plugins, position, layers, storageProvider, processingTimeService, parameters);
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
        return new GraphOperatorCoordinator.GraphOperatorCoordinatorProvider(position, layers, operatorID, graphOperatorSubCoordinatorsProvider);
    }

    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }
}
