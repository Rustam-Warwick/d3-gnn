package part;

import com.esotericsoftware.kryo.NotNull;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

abstract public class BasePart<VT extends BaseVertex> extends ProcessFunction<GraphQuery,GraphQuery> {
    public transient GraphStorage<VT> storage;
    public transient Short partId=null;
    public transient Collector<GraphQuery> out = null;
    public BasePart(GraphStorage<VT> a){
       this.setStorage(a);
    }
    public BasePart(){
        this.storage = null;
    }

    public GraphStorage<VT> getStorage() {
        return storage;
    }
    abstract public GraphStorage<VT> newStorage();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.setPartId((short)this.getRuntimeContext().getIndexOfThisSubtask());
        this.setStorage(this.newStorage());
    }

    public void setStorage(GraphStorage<VT> storage) {
        this.storage = storage;
        storage.setPart(this);
    }

    public Short getPartId() {
        return partId;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }
}
