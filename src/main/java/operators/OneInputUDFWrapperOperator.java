package operators;

import elements.GraphOp;
import functions.gnn_layers.GNNLayerFunction;
import iterations.MessageCommunication;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

public class OneInputUDFWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, GNNLayerFunction> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {


    public OneInputUDFWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers);
        getWrappedOperator().getUserFunction().setWrapperContext(context);
    }

    /**
     * Broadcast elements should go to all parts
     */
    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
            for (short part : thisParts) {
                element.getValue().setPartId(part);
                setKeyContextElement(element);
                getWrappedOperator().processElement(element);
            }
        } else {
            getWrappedOperator().processElement(element);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        getWrappedOperator().processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processActualWatermark(Watermark mark) throws Exception {
        getWrappedOperator().processWatermark(mark);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        getWrappedOperator().processLatencyMarker(latencyMarker);
    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        getWrappedOperator().setKeyContextElement(record);
    }


}
