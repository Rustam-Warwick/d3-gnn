package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

/**
 * Operator Factory for {@link IterationHeadOperator}
 * @param <OUT> Output Type for the Iterations
 */
public class IterationHeadOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> implements YieldingOperatorFactory<OUT> {

    /**
     * Mailbox Executor of the corresponding Task
     */
    transient MailboxExecutor mailboxExecutor;

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    final int headIterationId;

    public IterationHeadOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        return (T) new IterationHeadOperator<OUT>(headIterationId, mailboxExecutor);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationHeadOperator.class;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
    }
}
