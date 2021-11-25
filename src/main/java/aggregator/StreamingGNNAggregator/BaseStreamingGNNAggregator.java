package aggregator.StreamingGNNAggregator;

import aggregator.BaseAggregator;
import aggregator.GNNAggregator.GNNQuery;
import edge.BaseEdge;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import scala.Tuple4;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We can use aggregator functions such as SUM, MEAN, MAX , MIN. For MEAN we are sending the count accumulator
 * @param <VT> Vertex Type
 */
abstract public class BaseStreamingGNNAggregator<VT extends BaseVertex> extends BaseAggregator<VT> {
    /**
     * Level of aggregation
     */
    public final Short L;

    public BaseStreamingGNNAggregator(){
        this.L = 0;
    }
    public BaseStreamingGNNAggregator(Short L){
        this.L = L;
    }

    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return o.op== GraphQuery.OPERATORS.AGG;
    }


    abstract public INDArray UPDATE(INDArray aggregation,INDArray feature);

    /**
     * Given current aggregation value and acumulation and new message return new aggregation and acumulator value
     * @param aggregation
     * @param acummulator
     * @param newMessage
     * @return
     */
    abstract public Tuple2<INDArray,INDArray> COMBINE(INDArray aggregation,INDArray acummulator,INDArray newMessage);

    /**
     * Create a message from source and destination parameters
     * @param source
     * @param destination
     * @return
     */
    abstract public INDArray MESSAGE(INDArray source, INDArray destination);

    public CompletableFuture<INDArray> handleSourceDestinationPair(VT myDest, VT prevSource, VT prevDest){
        return CompletableFuture.allOf(myDest.getAggegation(this.L).getValue(),myDest.getAccumulator(this.L).getValue()).thenApply((vd)->{
            INDArray message = this.MESSAGE(prevSource.getFeature((short) (L-1)).getValue().join(),prevDest.getFeature((short) (L-1)).getValue().join());
            Tuple2<INDArray,INDArray> res = this.COMBINE(myDest.getAggegation(L).getValue().join(),myDest.getAccumulator(L).getValue().join(),message);
            INDArray updateFeature = this.UPDATE(res._1,prevDest.getFeature((short)(L-1)).getValue().join());
            myDest.getAggegation(L).setValue(res._1);
            myDest.getAccumulator(L).setValue(res._2);
            myDest.getFeature(L).setValue(updateFeature);
            return updateFeature;
        });
    }


    @Override
    public void dispatch(GraphQuery msg) {
        if(msg.element instanceof StreamingAggType){
            StreamingAggType<VT> query = (StreamingAggType<VT>) msg.element;
            if(query.destination==null){
                // We need to aggregate for all destinations of the given source
            }
            else{
                // Just specific source -> destination pair is what we need
                VT vertex = this.getPart().getStorage().getVertex(query.source.getId());
                VT dest = this.getPart().getStorage().getVertex(query.destination.getId());
                assert !Objects.isNull(vertex) && !Objects.isNull(dest);
                handleSourceDestinationPair(dest,query.source, query.destination)
                    .whenComplete((res,trw)->{

                    });
                // @todo Issue is that we need to pass in the destination from the previous layer as well
                // @solution Catch the Sync values and when they come send individually from each part
            }
        }
    }
}
