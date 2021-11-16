package part;

import aggregator.BaseAggregator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.Arrays;

abstract public class BasePart<VT extends BaseVertex> extends ProcessFunction<GraphQuery,GraphQuery> {
    public transient GraphStorage<VT> storage = null;
    public transient Short partId=null;
    public transient Collector<GraphQuery> out = null;
    public transient ArrayList<BaseAggregator<VT>> aggFunctions = null;
    public BasePart(GraphStorage<VT> a){
       this.setStorage(a);
    }
    public BasePart(){

    }

    public GraphStorage<VT> getStorage() {
        return storage;
    }
    abstract public GraphStorage<VT> newStorage();
    abstract public BaseAggregator<VT>[] newAggregators();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
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

    public Short getPartId() {
        return partId;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }
}
