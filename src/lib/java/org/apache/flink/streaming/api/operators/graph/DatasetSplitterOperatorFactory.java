package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.util.Preconditions;

/**
 * {@link AbstractStreamOperatorFactory} for {@link DatasetSplitterOperator}
 * <p>
 * This is always located at the start of the operator chain hence does not requre explicit position. It is always 0
 * </p>
 */
public class DatasetSplitterOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements CoordinatedOperatorFactory<GraphOp>, OneInputStreamOperatorFactory<GraphOp, GraphOp>, ProcessingTimeServiceAware {

    /**
     * Main Splitter process UDF
     */
    protected final KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction;

    /**
     * {@link org.apache.flink.streaming.api.operators.graph.GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider} if exists
     */
    protected final GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider;

    /**
     * Number of layers in the GNN pipeline
     */
    protected final short layers;

    /**
     * Process timer services
     */
    protected transient ProcessingTimeService processingTimeService;


    public DatasetSplitterOperatorFactory(short layers, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction, GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
        Preconditions.checkNotNull(processFunction);
        this.processFunction = processFunction;
        this.graphOperatorSubCoordinatorsProvider = graphOperatorSubCoordinatorsProvider;
        this.layers = layers;
    }

    public DatasetSplitterOperatorFactory(short layers, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction) {
        this(layers, processFunction, new GraphOperatorCoordinator.DefaultGraphOperatorSubCoordinatorsProvider());
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new GraphOperatorCoordinator.GraphOperatorCoordinatorProvider((short) 0, layers, operatorID, graphOperatorSubCoordinatorsProvider);
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        // Mailbox executor should have priority -1 else operator events will not be processed on training state
        return (T) new DatasetSplitterOperator(layers, processFunction, processingTimeService, parameters.getContainingTask().getMailboxExecutorFactory().createExecutor(-1), parameters);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DatasetSplitterOperator.class;
    }

    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }
}
