package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.*;

/**
 * Operator Factory for {@link IterationHeadOperator}
 * @param <OUT> Output Type for the Iterations
 */
public class IterationHeadOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> implements YieldingOperatorFactory<OUT>, OneInputStreamOperatorFactory<OUT,OUT> {

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    final int headIterationId;

    public IterationHeadOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        return (T) new IterationHeadOperator<>(headIterationId, getMailboxExecutor(), parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationHeadOperator.class;
    }

}
