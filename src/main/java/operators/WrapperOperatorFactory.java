package operators;

import elements.GraphOp;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.operators.*;

public class WrapperOperatorFactory extends AbstractStreamOperatorFactory<GraphOp> implements OneInputStreamOperatorFactory<GraphOp, GraphOp> {
    protected StreamOperator<GraphOp> innerOperator;
    protected IterationID iteraitonId;
    public WrapperOperatorFactory(StreamOperator<GraphOp> innerOperatorFactory, IterationID iterationId){
        this.innerOperator = innerOperatorFactory;
        this.iteraitonId = iterationId;
    }

    @Override
    public <T extends StreamOperator<GraphOp>> T createStreamOperator(StreamOperatorParameters<GraphOp> parameters) {
        if(innerOperator.getClass().isAssignableFrom(KeyedProcessOperator.class)){
            return (T) new OneInputUDFWrapperOperator(parameters, new SimpleUdfStreamOperatorFactory((KeyedProcessOperator) innerOperator), iteraitonId);
        }
        return null;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        if(innerOperator.getClass().isAssignableFrom(KeyedProcessOperator.class)){
            return KeyedProcessOperator.class;
        }
        return null;
    }
}
