package functions;

import elements.GraphElement;
import elements.GraphOp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.Objects;

public class GraphProcessFn extends HashMapStorage {
    public Collector<GraphOp> out;
    public short position = 1;
    public short layers = 1;

    public GraphProcessFn(short position, short layers){
        this.position = position;
        this.layers = layers;
    }
    public boolean isLast(){
        return this.position >= this.layers;
    }
    public boolean isFirst(){
        return this.position == 1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.partId = (short) this.getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void message(GraphOp op) {
        this.out.collect(op);
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<Short, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.out = out;
        if(!this.isLast()){

        }
        switch (value.op){
            case COMMIT:
                GraphElement thisElement = this.getElement(value.element);
                if(Objects.isNull(thisElement)){
                    thisElement = value.element.copy();
                    thisElement.setStorage(this);
                    thisElement.createElement();
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


        }
    }
}
