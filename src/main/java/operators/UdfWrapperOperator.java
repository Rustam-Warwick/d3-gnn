package operators;

import elements.GraphOp;
import elements.iterations.MessageCommunication;
import functions.gnn_layers.GNNLayerFunction;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class UdfWrapperOperator<T extends AbstractUdfStreamOperator<GraphOp, GNNLayerFunction> & OneInputStreamOperator<GraphOp, GraphOp>> extends BaseWrapperOperator<T> implements OneInputStreamOperator<GraphOp, GraphOp> {

    public UdfWrapperOperator(StreamOperatorParameters<GraphOp> parameters, StreamOperatorFactory<GraphOp> operatorFactory, IterationID iterationID, short position, short totalLayers) {
        super(parameters, operatorFactory, iterationID, position, totalLayers, (short) 3);
        getWrappedOperator().getUserFunction().setWrapperContext(context);
    }

    /**
     * Broadcast elements should go to all parts
     */
    @Override
    public void processActualElement(StreamRecord<GraphOp> element) throws Exception {
        if (element.getValue().getMessageCommunication() == MessageCommunication.BROADCAST) {
            // Broadcast messages invoked in all the parts
            for (short part : thisParts) {
                element.getValue().setPartId(part);
                setKeyContextElement(element);
                getWrappedOperator().processElement(element);
            }
        } else {
            getWrappedOperator().processElement(element);
        }
    }

//    /**
//     * Watermark came with some iteration number
//     *
//     * @param mark Watermark
//     */
//    @Override
//    public void processActualWatermark(Watermark mark) throws Exception {
//        getWrappedOperator().processWatermark(mark);
////        short iterationNumber = context.element.getValue().getPartId();
////        if (iterationNumber == 1) {
////            handleOperatorEvent(new ElementsSynced());
////        } else if (iterationNumber == 0) {
////            handleOperatorEvent(new ActionTaken());
////        }
//    }
//
//    /**
//     * Watermark Status came with some iteration number
//     *
//     * @param status Watermark Status
//     */
//    @Override
//    public void processActualWatermarkStatus(WatermarkStatus status) throws Exception {
//        getWrappedOperator().processWatermarkStatus(status);
//        short iterationNumber = context.element.getValue().getPartId();
//        if (iterationNumber == 1) {
//            handleOperatorEvent(new ElementsSynced());
//        } else if (iterationNumber == 0) {
//            handleOperatorEvent(new ActionTaken());
//            if (status == WatermarkStatus.IDLE && context.getPosition() == context.getNumLayers()) {
//                // This is an indication of training starting on the output layer, think it is starting training again
//                System.out.println("StartTraining");
//                handleOperatorEvent(new StartTraining());
//            }
//        }
//    }

    @Override
    public void setKeyContextElement(StreamRecord<GraphOp> record) throws Exception {
        super.setKeyContextElement(record);
        getWrappedOperator().setKeyContextElement(record);
    }

}
