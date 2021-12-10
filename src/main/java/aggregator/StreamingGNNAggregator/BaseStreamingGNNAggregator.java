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
abstract public class BaseStreamingGNNAggregator<V extends BaseVertex, T extends BaseEdge<V>,K> extends BaseAggregator<V,T> {

    public void toNextLevel(K update, BaseVertex destination){
        try{
            if(update==null) throw new NullPointerException();
            Feature.Update<K> a = new Feature.Update<>();
            a.setAttachedId(destination.getId());
            a.setFieldName(destination.getFeatureField((getPart().level + 1)).getName());
            a.setAttachedToClassName(destination.getClass().getName());
            a.setState(ReplicableGraphElement.STATE.NONE);
            a.setValue(update);
            GraphQuery query = new GraphQuery(a).changeOperation(GraphQuery.OPERATORS.UPDATE);
            this.getPart().collect(query.toPart(this.getPart().getPartId()),true);
        }
       catch (Exception e){
            e.printStackTrace();
       }
    }
    /**
     * Generates message for the given edge
     * @param e
     * @return
     */
    abstract public CompletableFuture<K> message(T e);
    abstract public CompletableFuture<K> update(V e);

    @Override
    public void addEdgeCallback(T edge) {
        super.addEdgeCallback(edge);
        this.message(edge).thenCompose(res->(
                this.update(edge.destination)
                )).whenComplete((res,vid)->{
                    this.toNextLevel(res,edge.destination);
        });
    }

    @Override
    public void updateVertexCallback(V vertex) {
        super.updateVertexCallback(vertex);
        getPart().getStorage().getEdges().filter(item->item.source.equals(vertex)).forEach(item->{
            T edge = (T) item;
            this.message(edge).thenCompose(res->this.update(edge.destination))
                    .whenComplete((res,vd)->{
                        this.toNextLevel(res,item.destination);
            });


            this.update(vertex).whenComplete((res,vd)->{
               this.toNextLevel(res,vertex);
            });
        });
    }

}
