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
        if(level==0){
            return CompletableFuture.allOf(e.feature.getValue(),e.source.feature.getValue()).thenApply((b)->{
                INDArray source = e.source.feature.getValue().join();
                INDArray edge = e.feature.getValue().join();
                return source.mul(edge);
            });
        }
        if(level==1){
            return CompletableFuture.allOf(e.feature.getValue(),e.source.h1.getValue()).thenApply((b)->{
                INDArray source = e.source.h1.getValue().join();
                INDArray edge = e.feature.getValue().join();
                return source.mul(edge);
            });
        }
        else{
            return CompletableFuture.allOf(e.feature.getValue(),e.source.h2.getValue()).thenApply((b)->{
                INDArray source = e.source.h2.getValue().join();
                INDArray edge = e.feature.getValue().join();
                return source.mul(edge);
            });

        }
    }

    @Override
    public CompletableFuture<INDArray> update(SimpleVertex e) {
        short level = getPart().level;
        if(level==0){
           return e.feature.getValue();
        }
        if(level==1){
            return e.h1.getValue();
        }
        return e.h2.getValue();
    }

}
