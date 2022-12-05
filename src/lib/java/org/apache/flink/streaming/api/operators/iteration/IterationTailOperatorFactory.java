package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.*;

public class IterationTailOperatorFactory<IN> extends AbstractStreamOperatorFactory<Void> implements OneInputStreamOperatorFactory<IN, Void>{

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    protected final int headIterationId;

    public IterationTailOperatorFactory(int headIterationId) {
        this.headIterationId = headIterationId;
    }

    @Override
    public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> parameters) {
        final IterationTailOperator<IN> finalOperator = new IterationTailOperator<IN>(headIterationId, parameters.getProcessingTimeService());
        finalOperator.setup(parameters.getContainingTask(),parameters.getStreamConfig(), parameters.getOutput());
        return (T) finalOperator;
    }


    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return IterationTailOperator.class;
    }

}
