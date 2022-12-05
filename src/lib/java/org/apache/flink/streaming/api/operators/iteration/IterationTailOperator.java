package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * TAIL Operator for handling Stream Iterations
 */
public class IterationTailOperator<IN> extends AbstractStreamOperator<Void> implements OneInputStreamOperator<IN, Void> {

    /**
     * ID Of the Head iteration to identify the buffer
     */
    protected final int headIterationId;

    public IterationTailOperator(int headIterationId, ProcessingTimeService processingTimeService) {
        this.headIterationId = headIterationId;
        this.processingTimeService = processingTimeService;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

    }
}
