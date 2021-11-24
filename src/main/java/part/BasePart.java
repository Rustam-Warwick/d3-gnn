package part;

import aggregator.BaseAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import partitioner.BasePartitioner;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.Arrays;

abstract public class BasePart<VT extends BaseVertex> extends ProcessFunction<GraphQuery,GraphQuery>  {
    public transient GraphStorage<VT> storage = null;
    public transient Short partId=null;
    public transient Collector<GraphQuery> out = null;
    public transient ArrayList<BaseAggregator<VT>> aggFunctions = null;
    public BasePart(GraphStorage<VT> a){
       this.setStorage(a);
    }
    public transient Integer count;
    public BasePart(){

    }

    public GraphStorage<VT> getStorage() {
        return storage;
    }
    abstract public GraphStorage<VT> newStorage();
    abstract public BaseAggregator<VT>[] newAggregators();
    abstract public void dispatch(GraphQuery g);
    public void collect(GraphQuery e){
        if(e.part!=null && e.part.equals(this.partId)){
            // Inteneded for this guy
            this.dispatch(e);
            return;
        }
        this.out.collect(e);
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.count = new Integer(0);
        this.setPartId((short)this.getRuntimeContext().getIndexOfThisSubtask());
        this.setStorage(this.newStorage());
        this.aggFunctions = new ArrayList<>();
        Arrays.stream(this.newAggregators()).forEach(this::attachAggregator);
    }

    public void setStorage(GraphStorage<VT> storage) {
        this.storage = storage;
        storage.setPart(this);
    }
    public BasePart<VT> attachAggregator(BaseAggregator<VT> e){
        aggFunctions.add(e);
        e.attachedTo(this);
        return this;
    }
    public void detachAggregator(BaseAggregator<VT> e){
        aggFunctions.remove(e);
    }


    @Override
    public void processElement(GraphQuery value, ProcessFunction<GraphQuery, GraphQuery>.Context ctx, Collector<GraphQuery> out) throws Exception {
        this.out = out;
        this.dispatch(value);
    }


    public Short getPartId() {
        return partId;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

    public static DataStream<GraphQuery> partHelper(DataStream<GraphQuery> source, BasePart x, FilterFunction<GraphQuery> iterateCondition, FilterFunction<GraphQuery> propegateCondition){
           IterativeStream<GraphQuery> iteration = source.iterate();
           DataStream<GraphQuery> tmp = iteration.partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
           DataStream<GraphQuery> res = tmp.process(x);
           DataStream<GraphQuery> filteredIteration = res.filter(iterateCondition).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
           iteration.closeWith(filteredIteration);
           DataStream<GraphQuery> filteredSend = res.filter(propegateCondition);
           return filteredSend;
    }
}
