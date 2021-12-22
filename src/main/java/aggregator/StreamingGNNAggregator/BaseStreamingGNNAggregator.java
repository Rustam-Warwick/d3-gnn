package aggregator.StreamingGNNAggregator;

import aggregator.BaseAggregator;
import edge.BaseEdge;
import org.tensorflow.types.family.TType;
import types.GraphQuery;
import types.TFWrapper;
import vertex.BaseVertex;
import java.util.concurrent.CompletableFuture;

/**
 * We can use aggregator functions such as SUM, MEAN, MAX , MIN. For MEAN we are sending the count accumulator
 *
 */
abstract public class BaseStreamingGNNAggregator<V extends BaseVertex, T extends BaseEdge<V>,K extends TType> extends BaseAggregator<V,T> {
    public Short level = 0;

    public class MSG{
        TFWrapper<K> feature;
        Short level;
        String id;

        public MSG(TFWrapper<K> feature, Short level, String id) {
            this.feature = feature;
            this.level = level;
            this.id = id;
        }
    }

    public void toNextLevel(TFWrapper<K> update, BaseVertex destination){
        try{
            if(update==null) throw new NullPointerException();
            MSG a = new MSG(update,level,destination.getId());
            GraphQuery query = new GraphQuery(a).changeOperation(GraphQuery.OPERATORS.AGG);
            getStorage().sendMessage(query);
        }
       catch (Exception e){
            e.printStackTrace();
       }
    }

    @Override
    public void dispatch(GraphQuery query) {
        super.dispatch(query);

    }

    /**
     * Generates message for the given edge
     * @param e
     * @return
     */
    abstract public CompletableFuture<TFWrapper<K>> message(T e);
    abstract public CompletableFuture<TFWrapper<K>> update(V e);



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
//        getPart().getStorage().getEdges().filter(item->item.source.equals(vertex)).forEach(item->{
//            T edge = (T) item;
//            this.message(edge).thenCompose(res->this.update(edge.destination))
//                    .whenComplete((res,vd)->{
//                        this.toNextLevel(res,item.destination);
//            });
//
//
//            this.update(vertex).whenComplete((res,vd)->{
//               this.toNextLevel(res,vertex);
//            });
//        });
    }

}
