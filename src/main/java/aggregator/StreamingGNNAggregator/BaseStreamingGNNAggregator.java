package aggregator.StreamingGNNAggregator;

import aggregator.BaseAggregator;
import aggregator.GNNAggregator.GNNQuery;
import edge.BaseEdge;
import features.Feature;
import javassist.NotFoundException;
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
 *
 */
abstract public class BaseStreamingGNNAggregator<V extends BaseVertex, T extends BaseEdge<V>> extends BaseAggregator {

    public void toNextLevel(INDArray update, BaseVertex destination){
        try{
            assert update!=null;
            Feature.Update<INDArray> a = new Feature.Update<>();
            a.setAttachedId(destination.getId());
            a.setFieldName(destination.getFeature((getPart().level + 1)).fieldName);
            a.setAttachedToClassName(destination.getClass().getName());
            a.setState(ReplicableGraphElement.STATE.NONE);
            a.setValue(update);
            GraphQuery query = new GraphQuery(a).changeOperation(GraphQuery.OPERATORS.UPDATE);
            this.getPart().collect(query.toPart(this.getPart().getPartId()),true);
        }
       catch (Exception e){
           System.out.println(e.getMessage());
       }
    }
    /**
     * Generates message for the given edge
     * @param e
     * @return
     */
    abstract public CompletableFuture<INDArray> message(T e);
    abstract public CompletableFuture<INDArray> update(V e);

    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return o.op== GraphQuery.OPERATORS.UPDATE || o.op== GraphQuery.OPERATORS.SYNC || o.op== GraphQuery.OPERATORS.ADD;
    }

    @Override
    public void dispatch(GraphQuery msg) {
        try{
            switch (msg.op){
                case ADD:{
                    if(msg.element instanceof BaseEdge){
                        // Edge Addition.
                        T edge = (T) msg.element;
                        //  Compute message -> Increment aggregator -> Send the UPDATED result from master partition only
                        this.message(edge).thenCompose(res->{
                            return this.update(edge.destination);
                        }).whenComplete((res,vid)->{
                            this.toNextLevel(res,edge.destination);
                        });
                    }
                    break;
                }

                case UPDATE:
                case SYNC:{
                    if(msg.element instanceof Feature.Update){
                        Feature.Update upd = (Feature.Update) msg.element;
                        V vertex = (V) getPart().getStorage().getVertex(upd.attachedId);
                        getPart().getStorage().getEdges().filter(item->item.source.equals(vertex)).forEach(item->{
                            T edge = (T) item;
                            this.message(edge).thenCompose(res->this.update(edge.destination))
                                    .whenComplete((resa,vod)->{
                                        this.toNextLevel(resa,item.destination);
                            });
                        });

                        this.update(vertex).whenComplete((resa,vod)->{
                            this.toNextLevel(resa,vertex);
                        });
                        }
                    }
                }
            }
        catch (NotFoundException e){
            System.out.println(e);
        }



    }
}
