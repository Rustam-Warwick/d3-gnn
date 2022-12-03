package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/**
 * TAIL Operator for handling Stream Iterations
 * @param <OUT> Type of feedback stream
 */
public class IterationTailOperator<OUT> extends AbstractStreamOperator<OUT> {
    /**
     * ID Of the Head iteration to identify the buffer
     */
    final int headIterationId;

    final transient MailboxExecutor mailboxExecutor;

    public IterationTailOperator(int headIterationId, MailboxExecutor mailboxExecutor) {
        this.headIterationId = headIterationId;
        this.mailboxExecutor = mailboxExecutor;
    }
}
