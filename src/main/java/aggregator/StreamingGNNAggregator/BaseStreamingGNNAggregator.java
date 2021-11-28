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
    public final Short maxL;

    public BaseStreamingGNNAggregator(){
        this.L = 0;
        this.maxL = 1;
    }
    public BaseStreamingGNNAggregator(Short L,Short maxL){
        this.L = L;this.maxL = maxL;
    }




    abstract public INDArray UPDATE(INDArray aggregation,INDArray feature);

    /**
     * Given current aggregation value and acumulation and new message return new aggregation and acumulator value
     * @param aggregation
     * @param newMessage
     * @return
     */
    abstract public INDArray COMBINE(INDArray aggregation,INDArray newMessage);

    /**
     * Create a message from source and destination parameters
     * @param source
     * @param destination
     * @return
     */
    abstract public INDArray MESSAGE(INDArray source,INDArray destination,INDArray edgeFeature);
    public CompletableFuture<INDArray> message(BaseEdge e){
       return CompletableFuture.allOf(e.source.getFeature(this.L).getValue(),e.destination.getFeature(this.L).getValue(),e.getFeature(this.L).getValue())
           .thenApply(item->{
               INDArray source = e.source.getFeature(this.L).getValue().join();
               INDArray destination = e.destination.getFeature(this.L).getValue().join();
               INDArray edge = e.getFeature(this.L).getValue().join();
               return this.MESSAGE(source,destination,edge);
        });
    }
    public CompletableFuture<INDArray> handleSourceDestinationPair(VT myDest, INDArray message, INDArray prevFeature){
        return CompletableFuture.allOf(myDest.getAggregation(this.L).getValue()).thenApply((vd)->{
            INDArray a = COMBINE(myDest.getAggregation(L).getValue().join(),message);
            myDest.getAggregation(L).setValue(a);
            INDArray updateFeature = this.UPDATE(a,prevFeature);
            myDest.getFeature(L).setValue(updateFeature);
            return updateFeature;
        });
    }

    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return o.op== GraphQuery.OPERATORS.AGG || o.op== GraphQuery.OPERATORS.ADD;
    }

    @Override
    public void dispatch(GraphQuery msg) {
        switch (msg.op){
            case ADD:{
                if(msg.element instanceof BaseEdge){
                    // Edge Addition
                    BaseEdge<VT> edge = (BaseEdge<VT>) msg.element;
                    this.message(edge).whenComplete((res,tr)->{
                        StreamingAggType<VT> query = new StreamingAggType<>(res,edge.destination.getId(),edge.destination.getFeature(this.L).getValue().join());
                        GraphQuery graphQuery = new GraphQuery(query).changeOperation(GraphQuery.OPERATORS.AGG).toPart(this.getPart().getPartId());
                        this.getPart().collect(graphQuery,true);
                    });
                }
                break;
            }
        }

        if(msg.element instanceof StreamingAggType){
            StreamingAggType<VT> query = (StreamingAggType<VT>) msg.element;
            assert !Objects.isNull(query.destinationId);
                // Just specific source -> destination pair is what we need
            VT dest = this.getPart().getStorage().getVertex(query.destinationId);
            assert !Objects.isNull(dest);
            handleSourceDestinationPair(dest,query.message, query.prevFeature)
            .whenComplete((res,trw)->{

            });
        }
    }
}
