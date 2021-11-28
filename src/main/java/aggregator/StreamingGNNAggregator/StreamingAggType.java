package aggregator.StreamingGNNAggregator;

import org.nd4j.linalg.api.ndarray.INDArray;
import vertex.BaseVertex;
import vertex.SimpleVertex;

public class StreamingAggType<VT extends BaseVertex> {
    public INDArray message;
    public String destinationId;
    public INDArray prevFeature;
    public StreamingAggType(){
        message = null;
        destinationId = null;
        prevFeature = null;
    }
    public StreamingAggType(INDArray message,String destinationId,INDArray prevFeature){
        this.message = message;
        this.destinationId = destinationId;
        this.prevFeature = prevFeature;
    }


}
