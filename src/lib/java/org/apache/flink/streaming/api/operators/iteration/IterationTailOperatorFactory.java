package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class IterationTailOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> {

    /**
     * Mailbox Executor of the corresponding Task
     */
    transient MailboxExecutor mailboxExecutor;

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    final int headIterationId;

    public IterationTailOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        return (T) new IterationTailOperator<>(headIterationId, mailboxExecutor);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationTailOperator.class;
    }

}
