package operators;

import elements.GraphOp;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.operators.*;

public class WrapperOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements OneInputStreamOperatorFactory<GraphOp, GraphOp> {
    protected StreamOperator<GraphOp> innerOperator;
    protected IterationID iterationId;

    public WrapperOperatorFactory(StreamOperator<GraphOp> innerOperatorFactory, IterationID iterationId) {
        this.innerOperator = innerOperatorFactory;
        this.iterationId = iterationId;
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        StreamOperatorFactory<GraphOp> factory = SimpleOperatorFactory.of(innerOperator);
        if (innerOperator instanceof AbstractUdfStreamOperator && innerOperator instanceof OneInputStreamOperator) {
            return (T) new OneInputUDFWrapperOperator(parameters, factory, iterationId);
        }

        return null;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        if (innerOperator.getClass().isAssignableFrom(KeyedProcessOperator.class)) {
            return KeyedProcessOperator.class;
        }
        return null;
    }
}
