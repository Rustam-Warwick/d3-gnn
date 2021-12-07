package aggregator.StreamingGNNAggregator;

import edge.BaseEdge;
import edge.SimpleEdge;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import vertex.BaseVertex;
import vertex.SimpleVertex;

import java.util.concurrent.CompletableFuture;

public class StreamingGNNAggregator extends BaseStreamingGNNAggregator<SimpleVertex, SimpleEdge>{

    public StreamingGNNAggregator() {
        super();
    }


    @Override
    public CompletableFuture<INDArray> message(SimpleEdge e) {
        short level = getPart().level;
        return CompletableFuture.allOf(e.feature.getValue(),e.feature.getValue()).thenApply((b)->{
            INDArray source = e.source.feature.getValue().join();
            INDArray edge = e.feature.getValue().join();
            return source.mul(edge);
        });
    }

    @Override
    public CompletableFuture<INDArray> update(SimpleVertex e) {
        short level = getPart().level;
        return e.feature.getValue();
    }

}
