package operators;

import elements.GraphOp;
import functions.storage.StorageProcessFunction;
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
            if (((AbstractUdfStreamOperator<?, ?>) innerOperator).getUserFunction() instanceof StorageProcessFunction) {
                return (T) new StorageLayerWrapperOperator(parameters, factory, iterationId, position, totalLayers);
            }
            return (T) new HeadUdfWrapperOperator(parameters, factory, iterationId, position, totalLayers);
        }

        return null;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        if (innerOperator instanceof AbstractUdfStreamOperator && innerOperator instanceof OneInputStreamOperator) {
            if (((AbstractUdfStreamOperator<?, ?>) innerOperator).getUserFunction() instanceof StorageProcessFunction) {
                return StorageLayerWrapperOperator.class;
            }
            return HeadUdfWrapperOperator.class;
        }
        return BaseWrapperOperator.class;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new WrapperOperatorCoordinator.WrappedOperatorCoordinatorProvider(operatorID, position, totalLayers);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.ALWAYS;
    }
}
