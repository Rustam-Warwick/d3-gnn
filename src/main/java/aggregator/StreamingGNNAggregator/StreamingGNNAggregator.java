package aggregator.StreamingGNNAggregator;

import edge.BaseEdge;
import edge.SimpleEdge;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.op.core.Constant;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import scala.collection.immutable.Stream;
import vertex.BaseVertex;
import vertex.SimpleVertex;

import java.util.concurrent.CompletableFuture;

public class StreamingGNNAggregator extends BaseStreamingGNNAggregator<SimpleVertex, SimpleEdge, TFloat32>{

    public StreamingGNNAggregator() {
        super();
    }


    @Override
    public CompletableFuture<TFloat32> message(SimpleEdge e) {
        short level = getPart().level;
        if(level==0){
            return CompletableFuture.allOf(e.feature.getValue(),e.source.feature.getValue()).thenApply((b)->{
                TFloat32 source = e.source.feature.getValue().join();
                TFloat32 edge = e.feature.getValue().join();
                return source;
//                return source.mul(edge);
            });
        }
        if(level==1){
            return CompletableFuture.allOf(e.feature.getValue(),e.source.h1.getValue()).thenApply((b)->{
                TFloat32 source = e.source.h1.getValue().join();
                TFloat32 edge = e.feature.getValue().join();
                return source;
            });
        }
        else{
            return CompletableFuture.allOf(e.feature.getValue(),e.source.h2.getValue()).thenApply((b)->{
                TFloat32 source = e.source.h2.getValue().join();
                TFloat32 edge = e.feature.getValue().join();
                return source;
            });

        }
    }

    @Override
    public CompletableFuture<TFloat32> update(SimpleVertex e) {
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
