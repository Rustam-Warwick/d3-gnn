package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.*;

/**
 * Operator Factory for {@link IterationHeadOperator}
 * @param <OUT> Output Type for the Iterations
 */
public class IterationHeadOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> implements YieldingOperatorFactory<OUT>{

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    protected final int headIterationId;

    public IterationHeadOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        final IterationHeadOperator<OUT> finalOperator = new IterationHeadOperator<>(headIterationId, getMailboxExecutor(), parameters.getProcessingTimeService());
        finalOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        return (T) finalOperator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationHeadOperator.class;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

}
