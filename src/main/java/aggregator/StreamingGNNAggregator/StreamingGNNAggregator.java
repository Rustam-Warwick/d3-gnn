package aggregator.StreamingGNNAggregator;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import vertex.BaseVertex;

public class StreamingGNNAggregator<VT extends BaseVertex> extends BaseStreamingGNNAggregator<VT>{

    @Override
    public INDArray UPDATE(INDArray aggregation, INDArray feature) {
        return (aggregation.add(feature).div(2));
    }

    @Override
    public INDArray COMBINE(INDArray aggregation, INDArray newMessage) {
        return (aggregation.add(newMessage).div(2));
    }

    @Override
    public INDArray MESSAGE(INDArray source, INDArray destination) {
       return Nd4j.concat(0,source,destination);
    }
}
