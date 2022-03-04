package functions;

import elements.GraphElement;
import elements.GraphOp;
import iterations.Rpc;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.Objects;

public class GraphProcessFn extends HashMapStorage {
    public transient Collector<GraphOp> out;



    @Override
    public void message(GraphOp op) {
        this.out.collect(op);
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<Short, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentKey = ctx.getCurrentKey();
        this.out = out;
        if(!this.isLast() && value.isTopologyChange()){
            this.message(value.copy());
        }
        try{
            switch (value.op){
                case COMMIT:
                    GraphElement thisElement = this.getElement(value.element);
                    if(Objects.isNull(thisElement)){
                        value.element.setStorage(this);
                        value.element.createElement();
                    }
                    else{
                        thisElement.externalUpdate(value.element);
                    }
                    break;
                case SYNC:
                    GraphElement el = this.getElement(value.element);
                    if(Objects.isNull(el)){
                        el = value.element.copy();
                        el.setStorage(this);
                        el.createElement();
                    }
                    el.syncElement(value.element);
                    break;
                case RPC:
                    GraphElement rpcElement = this.getElement(value.element.getId(),value.element.elementType());
                    Rpc.execute(rpcElement, (Rpc) value.element);
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
