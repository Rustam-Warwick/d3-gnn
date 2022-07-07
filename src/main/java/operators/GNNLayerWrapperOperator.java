package operators;

import elements.GraphOp;
import functions.gnn_layers.GNNLayerFunction;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Wrapper around GNNLayerFunctions
 * @param <T>
 */
public class GNNLayerWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, GNNLayerFunction> & Triggerable<Object, VoidNamespace> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    public GNNLayerWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers);
        getWrappedOperator().getUserFunction().setWrapperContext(context);
    }

    @Override
    public void processActualElement(StreamRecord<GraphOp> element) throws Exception {
        getWrappedOperator().processElement(element);
    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        super.setKeyContextElement(record);
        getWrappedOperator().setKeyContextElement(record);
    }

}
