package aggregator.GNNAggregator;

import aggregator.BaseAggregator;
import edge.BaseEdge;
import javassist.NotFoundException;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import scala.Tuple4;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We can use aggregator functions such as SUM, MEAN, MAX , MIN. For MEAN we are sending the count accumulator
 * @param <VT> Vertex Type
 */
abstract public class BaseGNNAggregator<VT extends BaseVertex> extends BaseAggregator<VT> {
    public HashMap<String, Tuple4<Short, Short, String, ArrayList<Tuple2<INDArray,Integer>>>> gnnQueries = new HashMap<>();
    public transient INDArray identity = Nd4j.zeros(1);
    public static GraphQuery prepareQuery(GNNQuery e){
        return new GraphQuery(e).changeOperation(GraphQuery.OPERATORS.AGG);
    }

    /**
     * User defined aggregation function
     * @param sourceV Features of source vertex
     * @param destV Features of destination vertex
     * @param edge Features of the edge
     * @param l level of aggregation [0...L-1]
     * @return INDArray of the message values
     */
    abstract public INDArray MESSAGE(INDArray sourceV,INDArray destV,INDArray edge,short l );

    /**
     * User Defined accumulator function
     * @param m1 Message 1
     * @param m2 Message 2
     * @param accumulator Accumulator maybe to count the size of elements
     * @return
     */
    abstract public INDArray ACCUMULATE(INDArray m1,INDArray m2,AtomicInteger accumulator);

    /**
     * Combine the results of parallel ACCUMULATIONS into a single aggregation result
     * Note that the ACCUMULATIONS can be null in some parts
     * @param accumulations List of accumulations from various parallel instances
     * @return INDArray of Accumulation
     */
    abstract public INDArray COMBINER(ArrayList<Tuple2<INDArray,Integer>> accumulations );

    /**
     * UPDATE The vertex feature based on the aggregated neighborhood and its current value
     * @param featureNow
     * @param aggregations
     * @return
     */
    abstract public INDArray UPDATE(INDArray featureNow,INDArray aggregations);

    /**
     * Given L level aggregation of a vertex decide if propogation should be terminated or continued
     * @param lNow
     * @return
     */
    abstract public boolean stopPropagation(Short lNow,VT vertex);

    /**
     * Get the message of the given edge for aggregating destination l-hop
     * @param edge Edge
     * @param l short representing the neighborhood for which we are aggregating
     */

    public CompletableFuture<INDArray> message(BaseEdge<VT> edge, Short l){
        short lminus = (short) (l-1);
        CompletableFuture<INDArray>[] features = new CompletableFuture[]{edge.source.getFeature(lminus).getValue(), edge.destination.getFeature(lminus).getValue(),edge.getFeature(lminus).getValue()};
        return CompletableFuture.allOf(features).thenApply((vod)->(
            this.MESSAGE(features[0].join(),features[1].join(),features[2].join(),lminus)
        ));
    }
    public CompletableFuture<INDArray> accumulate(CompletableFuture<INDArray> f1,CompletableFuture<INDArray> f2, AtomicInteger acc){
        return CompletableFuture.allOf(f1,f2).thenApply((vod)->(
                this.ACCUMULATE(f1.join(),f2.join(),acc)
                ));
    }
    /**
     * Send a request message to all parts containing this vertex, so they do partial aggregation and send it back
     * @param l level of aggregation needed for the vertex
     * @param v vertex for which we are aggregating
     */
    public void startGNNCell(Short l, BaseVertex v){
         assert l>0;
         String id = UUID.randomUUID().toString();
         v.parts.getValue().whenComplete((parts,thr)->{
             gnnQueries.put(id, new Tuple4<>((short) parts.size(), l, v.getId(), new ArrayList<>()));
             GNNQuery query = new GNNQuery().withResponsePart(getPart().getPartId()).withId(id).withOperator(GNNQuery.OPERATORS.REQUEST).withLValue(l).withVertex(v.getId());
             GraphQuery gQuery = BaseGNNAggregator.prepareQuery(query);
             v.sendMessageToReplicas(gQuery,getPart().getPartId()); // Send aggregation request to master and all replicas available
         });
    }

    /**
     * Once all the partial aggregations are collected this function computes and update the L value of the vertex
     * If needed propagates the message and continue with L+1 aggregations
     * @param gnnState
     */
    public void continueGNNCell(Tuple4<Short,Short,String,ArrayList<Tuple2<INDArray,Integer>>> gnnState){
        gnnState._4().removeIf(item->item._2==0); // Clear empty ones
        INDArray combinedNeighAgg = this.COMBINER(gnnState._4());
        VT vertex = getPart().getStorage().getVertex(gnnState._3());
        vertex.getFeature(gnnState._2()).getValue().whenComplete((res,thr)->{
            INDArray updatedValue = this.UPDATE(res,combinedNeighAgg);
            vertex.getFeature(gnnState._2()).setValue(updatedValue);
            this.gnnQueries.remove(gnnState);
        });
    }

    public void interStepFunction(Short lNow, VT vertexUpdated){

    }

    /**
     * Once the request arrives to this part, this function partially aggregates all the messages of this vertex
     * and sends it back to requesting part
     * @param query
     * @return
     */
    public CompletableFuture<Tuple2<INDArray,Integer>> getLocalMessages(GNNQuery query){
        try{
            CompletableFuture<INDArray> reductionInitial  = new CompletableFuture<>();
            reductionInitial.complete(this.identity);
            VT vertex = part.getStorage().getVertex(query.vertexId);
            AtomicInteger acc = new AtomicInteger(0);
            CompletableFuture<INDArray> messages =  part.getStorage().getEdges()
                    .filter(item->item.destination.equals(vertex))
                    .map(item->this.message(item, query.l))
                    .reduce(reductionInitial,(m1,m2)->this.accumulate(m1,m2,acc));

            return messages.thenApply(aggregations-> {
               return new Tuple2<>(aggregations, acc.get());
            });


        }catch (Exception e){
            System.out.println(e);
            CompletableFuture<Tuple2<INDArray,Integer>> ftException = new CompletableFuture<>();
            ftException.complete(new Tuple2<>(this.identity,0));
            return ftException;
        }
    }




    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return (o.element instanceof GNNQuery && o.op == GraphQuery.OPERATORS.AGG) || o.op == GraphQuery.OPERATORS.ADD;
    }


    @Override
    public void dispatch(GraphQuery msg) {
        switch(msg.op){
            case AGG : {
                GNNQuery incomingQuery = (GNNQuery) msg.element;
                if(incomingQuery.op == GNNQuery.OPERATORS.REQUEST){
                    CompletableFuture<Tuple2<INDArray,Integer>> res = getLocalMessages(incomingQuery);
                    res.whenComplete((val,err)->{
                        Short response = incomingQuery.responsePart;
                        incomingQuery.withAggValue(val._1).withAccumulator(val._2).withResponsePart(getPart().getPartId()).withOperator(GNNQuery.OPERATORS.RESPONSE);
                        GraphQuery query = BaseGNNAggregator.prepareQuery(incomingQuery);
                        getPart().collect(query.generateQueryForPart(response),false);
                    });
                }
                else if(incomingQuery.op== GNNQuery.OPERATORS.RESPONSE){
                    // Response for my previous query
                    Tuple4<Short,Short,String,ArrayList<Tuple2<INDArray,Integer>>> gnnState = gnnQueries.get(incomingQuery.uuid);
                    gnnState._4().add(new Tuple2<>(incomingQuery.agg, incomingQuery.accumulator));
                    if(gnnState._4().size()>=gnnState._1()){
                        // Agg messages are ready
                        this.continueGNNCell(gnnState);
                    }
                }
                break;

            }
            case ADD:{
                if(msg.element instanceof BaseEdge){
                    BaseEdge<VT> tmp = (BaseEdge<VT>) msg.element;

                    this.startGNNCell((short) 1, this.part.getStorage().getVertex(tmp.destination.getId()));
                }
                break;
            }

        }
    }
}
