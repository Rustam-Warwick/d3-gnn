package operators;

import elements.GraphOp;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.operators.*;

public class WrapperOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements OneInputStreamOperatorFactory<GraphOp, GraphOp> {
    protected StreamOperator<GraphOp> innerOperator;
    protected IterationID iterationId;
    protected short position;
    protected short totalLayers;

    /**
     * @param innerOperatorFactory Operator to be wrapped by this
     * @param iterationId          iterationId of the operator
     * @param position             horizontal position of this operator
     * @param totalLayers          total number of layers in the ML chain
     */
    public WrapperOperatorFactory(StreamOperator<GraphOp> innerOperatorFactory, IterationID iterationId, short position, short totalLayers) {
        this.innerOperator = innerOperatorFactory;
        this.iterationId = iterationId;
        this.position = position;
        this.totalLayers = totalLayers;
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        StreamOperatorFactory<GraphOp> factory = SimpleOperatorFactory.of(innerOperator);
        if (innerOperator instanceof AbstractUdfStreamOperator && innerOperator instanceof OneInputStreamOperator) {
            return (T) new OneInputUDFWrapperOperator(parameters, factory, iterationId, position, totalLayers);
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
