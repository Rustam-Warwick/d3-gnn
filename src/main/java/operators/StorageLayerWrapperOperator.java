package operators;

import elements.GraphOp;
import functions.storage.StorageProcessFunction;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Wrapper around GNNLayerFunctions, functions that are part of the GNN Layer
 *
 * @param <T>
 */
public class StorageLayerWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, StorageProcessFunction> & Triggerable<Object, VoidNamespace> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    public StorageLayerWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
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
