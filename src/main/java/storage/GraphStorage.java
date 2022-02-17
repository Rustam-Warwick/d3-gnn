package storage;

import aggregator.BaseAggregator;
import edge.BaseEdge;
import features.Feature;
import javassist.NotFoundException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import part.BasePart;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.stream.Stream;

public abstract class GraphStorage extends ProcessFunction<GraphQuery,GraphQuery> {
    public transient Short partId = null; // Initialized on open()
    public transient volatile Collector<GraphQuery> out = null; // On each process updated
    public ArrayList<BaseAggregator> aggFunctions = null; // Serialized and sent over network

    public GraphStorage(BasePart part) {
    }

    /**
     * Main function for sending output to next operator
     * @param e
     * @param forceNextOperator
     */
    synchronized public void collect(GraphQuery e,boolean forceNextOperator){
        if(forceNextOperator){
            this.out.collect(e);
        }else{
            if(e.part.equals(this.partId)){
                this.dispatch(e);
            }
            else this.out.collect(e);
        }
    }

    public void sendMessage(GraphQuery p,Short part){
        this.collect(p.generateQueryForPart(part),false);
    }
    public void sendMessage(GraphQuery p,Short part, boolean forceNextOperator){
        this.collect(p.generateQueryForPart(part),forceNextOperator);
    }
    public void sendMessage(GraphQuery p){
        this.collect(p.generateQueryForPart(getPartId()),false);
    }
    public void sendMessage(GraphQuery p,boolean forceNextOperator){
        this.collect(p.generateQueryForPart(getPartId()),forceNextOperator);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.setPartId((short)this.getRuntimeContext().getIndexOfThisSubtask()); // Set this part id
        if(this.aggFunctions==null)this.aggFunctions = new ArrayList<>(); // Might be initilized from the serialization stack
        for(BaseAggregator i:this.aggFunctions){
            i.attachedTo(this);
        }
    }

    // CRUD Related Stuff
    abstract public BaseVertex addVertexMain(BaseVertex v);
    public BaseVertex addVertex(BaseVertex v){
        this.aggFunctions.forEach(item->item.preAddVertexCallback(v));
        BaseVertex tmp = this.addVertexMain(v);
        this.aggFunctions.forEach(item->item.addVertexCallback(v));
        return tmp;
    }
    abstract public BaseEdge<BaseVertex> addEdgeMain(BaseEdge<BaseVertex> e);
    public BaseEdge<BaseVertex> addEdge(BaseEdge<BaseVertex> e){
        this.aggFunctions.forEach(item->item.preAddEdgeCallback(e));
        BaseEdge<BaseVertex> tmp = this.addEdgeMain(e);
        this.aggFunctions.forEach(item->item.addEdgeCallback(e));
        return tmp;
    }

    abstract public void updateFeatureMain(Feature.Update<?> e);
    public void updateFeature(Feature.Update<?> e){

    }

    // Query Related Stuff
    abstract public BaseVertex getVertex(String id) throws NotFoundException;
    abstract public Stream<BaseVertex> getVertices();
    abstract public Stream<BaseEdge<BaseVertex>> getEdges();
    abstract public BaseEdge<BaseVertex> getEdge(String source,String dest) throws NotFoundException;


    /**
     * Controller main loop that haldes each type of graph Query
     * @param query input GraphQuery
     */
    public void dispatch(GraphQuery query){
        try {
            boolean isVertex = query.element instanceof BaseVertex;
            boolean isEdge = query.element instanceof BaseEdge;
            boolean isFeature = query.element instanceof Feature.Update;
            switch (query.op) {
                case ADD: {
                    if (isEdge) {
                        this.collect(query, true);
                        BaseEdge<BaseVertex> tmp = (BaseEdge) query.element;
                        this.addEdge(tmp);
                    }
                    break;
                }
                case REMOVE:
                    System.out.println("Remove Operation");
                case UPDATE: {
                    if (isFeature) {
                       this.updateFeature((Feature.Update<?>) query.element);
                    }
                    break;
                }
                case SYNC: {
                    if (isFeature) {
                        this.updateFeature((Feature.Update<?>) query.element);
                    }
                    break;
                }
                case AGG: {
                    // Handled below in aggFunctions
                    this.aggFunctions.forEach(item->item.dispatch(query));
                    break;
                }
                default:
                    System.out.println("Undefined Operation");
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
    };


    @Override
    public void processElement(GraphQuery value, ProcessFunction<GraphQuery, GraphQuery>.Context ctx, Collector<GraphQuery> out) throws Exception {
        this.out = out;
        this.dispatch(value);
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }
    public Short getPartId() {return partId;}
}
