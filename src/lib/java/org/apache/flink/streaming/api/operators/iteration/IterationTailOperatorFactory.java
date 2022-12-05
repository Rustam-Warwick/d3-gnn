package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class IterationTailOperatorFactory<IN> extends AbstractStreamOperatorFactory<Void> implements OneInputStreamOperatorFactory<IN, Void> {

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    final int headIterationId;

    public IterationTailOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> parameters) {
        return (T) new IterationTailOperator<IN>(headIterationId, parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationTailOperator.class;
    }

}
