package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/**
 * HEAD Operator for handling Stream Iterations
 * Acts like a source
 * @todo Add Termination Detection
 * @param <OUT> Output Type
 */
public class IterationHeadOperator<OUT> extends AbstractStreamOperator<OUT>{

    /**
     * Unique ID for the Iteration
     */
    final int headIterationId;

    /**
     * Mailbox Executor to attach to
     */
    final transient MailboxExecutor mailboxExecutor;

    public IterationHeadOperator(int headIterationId, MailboxExecutor mailboxExecutor) {
        this.headIterationId = headIterationId;
        this.mailboxExecutor = mailboxExecutor;
    }
}
