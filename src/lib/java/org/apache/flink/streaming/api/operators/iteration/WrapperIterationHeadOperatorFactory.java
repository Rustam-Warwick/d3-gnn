package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Optional;

/**
 * Operator Factory for {@link WrapperIterationHeadOperator}
 * This operator wraps the main body operator factory and adds HEAD logic on top of it
 * @param <OUT> Output Type of the Head Operator
 */
public class WrapperIterationHeadOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> implements YieldingOperatorFactory<OUT>, CoordinatedOperatorFactory<OUT>{

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    protected final int iterationID;

    /**
     * Main Body {@link StreamOperatorFactory}
     */
    protected StreamOperatorFactory<OUT> bodyOperatorFactory;

    public WrapperIterationHeadOperatorFactory(int iterationID, StreamOperatorFactory<OUT> bodyOperatorFactory) {
        this.iterationID = iterationID;
        this.bodyOperatorFactory = bodyOperatorFactory;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        final Tuple2<AbstractStreamOperator<OUT>, Optional<ProcessingTimeService>> bodyOperatorResult = StreamOperatorFactoryUtil.createOperator(bodyOperatorFactory,(StreamTask<OUT, ?>) parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput(), parameters.getOperatorEventDispatcher());
        return (T) new WrapperIterationHeadOperator<>(iterationID, getMailboxExecutor(), bodyOperatorResult.f0, parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return bodyOperatorFactory.getStreamOperatorClass(classLoader);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return bodyOperatorFactory.getChainingStrategy();
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        OperatorCoordinator.Provider bodyOperatorCoordinationProvider = null;
        if(bodyOperatorFactory instanceof CoordinatedOperatorFactory){
            bodyOperatorCoordinationProvider = ((CoordinatedOperatorFactory) bodyOperatorFactory).getCoordinatorProvider(operatorName, operatorID);
        }
        return new WrapperIterationHeadOperatorCoordinator.WrapperIterationHeadOperatorCoordinatorProvider(operatorID, bodyOperatorCoordinationProvider);
    }
}
