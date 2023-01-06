package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.util.Preconditions;

/**
 * {@link AbstractStreamOperatorFactory} for {@link DatasetSplitterOperator}
 * <p>
 * This is always located at the start of the operator chain hence does not requre explicit position. It is always 0
 * </p>
 */
public class DatasetSplitterOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements CoordinatedOperatorFactory<GraphOp>, OneInputStreamOperatorFactory<GraphOp, GraphOp> {

    /**
     * Main Splitter process UDF
     */
    protected final KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction;

    /**
     * {@link org.apache.flink.streaming.api.operators.graph.GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider} if exists
     */
    final protected GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider;

    public DatasetSplitterOperatorFactory(KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction, GraphOperatorCoordinator.GraphOperatorSubCoordinatorsProvider graphOperatorSubCoordinatorsProvider) {
        Preconditions.checkNotNull(processFunction);
        this.processFunction = processFunction;
        this.graphOperatorSubCoordinatorsProvider = graphOperatorSubCoordinatorsProvider;
    }

    public DatasetSplitterOperatorFactory(KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction) {
        this(processFunction, new GraphOperatorCoordinator.DefaultGraphOperatorSubCoordinatorsProvider());
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new GraphOperatorCoordinator.GraphOperatorCoordinatorProvider((short) 0, operatorID, graphOperatorSubCoordinatorsProvider);
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        return (T) new DatasetSplitterOperator(processFunction, parameters);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DatasetSplitterOperator.class;
    }
}
