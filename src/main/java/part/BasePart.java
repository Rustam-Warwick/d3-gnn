package part;

import aggregator.BaseAggregator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import partitioner.BasePartitioner;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.Arrays;

abstract public class BasePart<VT extends BaseVertex> extends KeyedProcessFunction<Short,GraphQuery,GraphQuery>  {
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
        count++;
        if(count%1000==0) System.out.println(count + " Messages sent");
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
    public void processElement(GraphQuery value, KeyedProcessFunction<Short, GraphQuery, GraphQuery>.Context ctx, Collector<GraphQuery> out) throws Exception {
        this.out = out;
        System.out.format("Part id: %s | Query part: %s \n",this.partId,value.part);
        this.dispatch(value);
    }


    public Short getPartId() {
        return partId;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

}
