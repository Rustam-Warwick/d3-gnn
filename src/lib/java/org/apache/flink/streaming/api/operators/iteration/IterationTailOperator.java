package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * TAIL Operator for handling Stream Iterations
 */
public class IterationTailOperator<IN> extends AbstractStreamOperator<Void> implements OneInputStreamOperator<IN, Void> {

    /**
     * ID Of the Head iteration to identify the buffer
     */
    final int headIterationId;

    public IterationTailOperator(int headIterationId, StreamOperatorParameters<Void> parameters) {
        this.headIterationId = headIterationId;
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

    }
}
