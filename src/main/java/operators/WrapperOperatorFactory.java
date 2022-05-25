package operators;

import elements.GraphOp;
import operators.coordinators.WrapperOperatorCoordinator;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;

public class WrapperOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements OneInputStreamOperatorFactory<GraphOp, GraphOp>, CoordinatedOperatorFactory<GraphOp> {
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
            if (position == 0)
                return (T) new UdfHeadWrapperOperator(parameters, factory, iterationId, position, totalLayers);
            else return (T) new UdfWrapperOperator(parameters, factory, iterationId, position, totalLayers);
        }

        return null;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return BaseWrapperOperator.class;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new WrapperOperatorCoordinator.HeadOperatorCoordinatorProvider(operatorID, position, totalLayers);
    }

}
