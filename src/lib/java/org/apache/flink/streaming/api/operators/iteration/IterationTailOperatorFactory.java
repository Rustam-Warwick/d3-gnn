package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.streaming.api.operators.*;

/**
 * Operator Factory for {@link IterationTailOperator}
 *
 * @param <IN> Input to be directed to the iteration head through feedback channel
 */
public class IterationTailOperatorFactory<IN> extends AbstractStreamOperatorFactory<Void> implements OneInputStreamOperatorFactory<IN, Void> {

    /**
     * ID of the HeadTransformation. To be combined with the jobId and attemptId for uniqueness
     */
    protected final int iterationID;

    /**
     * Are element with {@link ai.djl.ndarray.LifeCycleControl}
     */
    protected final boolean hasLifeCycleControl;

    public IterationTailOperatorFactory(int iterationID, boolean hasLifeCycleControl) {
        this.iterationID = iterationID;
        this.hasLifeCycleControl = hasLifeCycleControl;
    }

    @Override
    public <T extends StreamOperator<Void>> T createStreamOperator(StreamOperatorParameters<Void> parameters) {
        return (T) new IterationTailOperator<IN>(iterationID, parameters, hasLifeCycleControl);
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
