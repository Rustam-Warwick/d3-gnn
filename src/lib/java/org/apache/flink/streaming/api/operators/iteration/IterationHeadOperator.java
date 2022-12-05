package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * HEAD Operator for handling Stream Iterations
 * Acts like a source
 * @todo Add Termination Detection
 * @param <OUT> Output Type
 */
public class IterationHeadOperator<OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<OUT, OUT> {

    /**
     * Unique ID for the Iteration
     */
    final int headIterationId;

    /**
     * Mailbox Executor to attach to
     */
    final transient MailboxExecutor mailboxExecutor;

    public IterationHeadOperator(int headIterationId, MailboxExecutor mailboxExecutor, StreamOperatorParameters<OUT> parameters) {
        this.headIterationId = headIterationId;
        this.mailboxExecutor = mailboxExecutor;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void processElement(StreamRecord<OUT> element) throws Exception {

    }
}
