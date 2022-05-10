package operators;

import com.codahale.metrics.MetricRegistryListener;
import elements.GraphOp;
import elements.Op;
import functions.gnn_layers.GNNLayerFunction;
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
    private boolean watermarkInIteration = false;
    private Watermark waitingWatermark = null;
    public OneInputUDFWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID) {
        super(parameters, operatorFactory, iterationID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        getWrappedOperator().getUserFunction().setWrapperContext(context);
    }

    @Override
    public void processElement(StreamRecord<GraphOp> element) throws Exception {
        getWrappedOperator().processElement(element);
    }

    @Override
    public void processFeedback(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().op == Op.WATERMARK) {
            short iterationNumber = (short) (element.getTimestamp() % 4);
            Watermark newWatermark = new Watermark(element.getTimestamp() + 1);
            if (iterationNumber < 2) {
                // Still need to traverse the stream before updating the timer
                context.emitWatermark(BaseWrapperOperator.iterateOutputTag, newWatermark);
            } else {
                // Watermark is ready to be consumed, before consuming do onWatermark on all the keyed elements
                getWrappedOperator().processWatermark(newWatermark);
                watermarkInIteration = false;
                if (waitingWatermark != null) {
                    processWatermark(waitingWatermark);
                    waitingWatermark = null;
                }
            }
        } else {
            processElement(element);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (watermarkInIteration) {
            if (waitingWatermark == null) waitingWatermark = mark;
            else {
                waitingWatermark = new Watermark(Math.max(waitingWatermark.getTimestamp(), mark.getTimestamp()));
            }
        } else {
            Watermark iterationWatermark = new Watermark(mark.getTimestamp() - (mark.getTimestamp() % 4)); // Normalize to have remainder 0
            watermarkInIteration = true;
            context.emitWatermark(BaseWrapperOperator.iterateOutputTag, iterationWatermark);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        getWrappedOperator().processWatermarkStatus(watermarkStatus);
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
