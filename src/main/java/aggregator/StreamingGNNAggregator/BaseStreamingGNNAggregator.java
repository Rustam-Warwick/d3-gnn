package aggregator.StreamingGNNAggregator;

import aggregator.BaseAggregator;
import aggregator.GNNAggregator.GNNQuery;
import edge.BaseEdge;
import features.Feature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import scala.Tuple4;
import types.GraphQuery;
import types.ReplicableGraphElement;
import vertex.BaseVertex;

import java.util.*;
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

    /**
     * Update is triggered on master node only. To avoid redundant computations
     * @param vertex vertex to be updated
     * @return
     */
    public void update(VT vertex){
        CompletableFuture.allOf(vertex.getFeature(this.L).getValue(),vertex.getAggregation(this.L).getValue())
            .whenComplete((none,err)->{
               INDArray updatedNextFeature = this.UPDATE(vertex.getAggregation(this.L).getValue().join(),vertex.getFeature(this.L).getValue().join());
               Feature.Update<INDArray> a = new Feature.Update<INDArray>();
               a.setAttachedId(vertex.getId());
               a.setFieldName("h"+(this.L+1));
               a.setAttachedToClassName(vertex.getClass().getName());
               a.setState(ReplicableGraphElement.STATE.NONE);
               a.setValue(updatedNextFeature);
               GraphQuery query = new GraphQuery(a).changeOperation(GraphQuery.OPERATORS.UPDATE);
               if(vertex.getState()== ReplicableGraphElement.STATE.MASTER){
                   this.getPart().collect(query.toPart(vertex.getPartId()),true);
               }else{
                   this.getPart().collect(query.toPart(vertex.getMasterPart()),true);
               }
            });
    }

    /**
     * Generates message for the given edge
     * @param e
     * @return
     */
    public CompletableFuture<INDArray> message(BaseEdge<VT> e){
       return CompletableFuture.allOf(e.source.getFeature(this.L).getValue(),e.destination.getFeature(this.L).getValue(),e.getFeature(this.L).getValue())
           .thenApply(item->{
               INDArray source = e.source.getFeature(this.L).getValue().join();
               INDArray destination = e.destination.getFeature(this.L).getValue().join();
               INDArray edge = e.getFeature(this.L).getValue().join();
               return this.MESSAGE(source,destination,edge);
        });
    }

    public void updateOutNeighbors(VT vertex){
        getPart().getStorage().getEdges().filter(item->item.source.equals(vertex)).forEach(edge->{
                    this.message(edge)
                        .whenComplete((msg,trw)->{
                            edge.destination.getAggregation(this.L).updateElements(msg);
                            this.update(edge.destination);
                        });
                });
    }
    public void updateSelf(VT vertex){
        Object[] messages =  getPart().getStorage().getEdges().filter(item->item.destination.equals(vertex)).map(this::message).toArray();
        CompletableFuture<INDArray>[] futures = new CompletableFuture[messages.length];
        for(int i=0;i<messages.length;i++){
            futures[i] = (CompletableFuture<INDArray>) messages[i];
        }
        CompletableFuture.allOf(futures)
            .thenApply((vois)->(
               (INDArray[]) Arrays.stream(futures).map(CompletableFuture::join).toArray()
            ))
            .whenComplete((item,err)->{
                vertex.getAggregation(this.L).updateElements(item);
                this.update(vertex);
            });
    }

    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return o.op== GraphQuery.OPERATORS.UPDATE || o.op== GraphQuery.OPERATORS.SYNC ||o.op== GraphQuery.OPERATORS.ADD;
    }

    @Override
    public void dispatch(GraphQuery msg) {
        switch (msg.op){
            case ADD:{
                if(msg.element instanceof BaseEdge){
                    // Edge Addition.
                    BaseEdge<VT> edge = (BaseEdge<VT>) msg.element;
                    //  Compute message -> Increment aggregator -> Send the UPDATED result from master partition only
                    this.message(edge).whenComplete((item,err)->{
                       edge.destination.getAggregation(this.L).addNewElement(item);
                       this.update(edge.destination);
                    });
                }
                break;
            }
            case UPDATE:
            case SYNC:{
               if(msg.element instanceof Feature.Update){
                   Feature.Update upd = (Feature.Update) msg.element;
                   if(upd.fieldName.equals("h"+this.L)){
                       // Feature update incoming
                       VT vertex = getPart().getStorage().getVertex(upd.attachedId);
                       this.updateSelf(vertex);
                       this.updateOutNeighbors(vertex);
                   }
               }
            }
        }


    }
}
