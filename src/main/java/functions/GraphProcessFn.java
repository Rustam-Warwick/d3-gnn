package functions;

import elements.*;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;

import java.util.List;
import java.util.Objects;

public class GraphProcessFn extends HashMapStorage {
    public transient Collector<GraphOp> out;

    @Override
    public void message(GraphOp op) {
//
//        if(this.position==2 && op.element.elementType() == ElementType.VERTEX && op.element.getId().equals("434")){
//            System.out.println("s");
//        }
//        if(this.position==2 && op.element.elementType() == ElementType.FEATURE && ((Feature)op.element).attachedTo._2.equals("434")){
//            System.out.println("s");
//        }
        this.out.collect(op);
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.currentKey = Short.parseShort(ctx.getCurrentKey());
        if(value.op != Op.RPC){
            if(this.isLast() && value.element.elementType()==ElementType.EDGE && List.of("434").contains(((Edge)value.element).dest.getId())){
                System.out.println();
            }
            if(this.isLast() && value.element.elementType()==ElementType.VERTEX && List.of("114","10798", "8703", "6213", "4649").contains(value.element.getId())){
                System.out.println();
            }
            if(this.isLast() && value.element.elementType()==ElementType.FEATURE && List.of("114","10798", "8703", "6213", "4649").contains(((Feature)value.element).attachedTo._2)){
                System.out.println();
            }
        }

        this.out = out;
        try{
            switch (value.op){
                case COMMIT:
                    GraphElement thisElement = this.getElement(value.element);
                    if(Objects.isNull(thisElement)){
                        if(!this.isLast() && (value.element.elementType()== ElementType.EDGE || value.element.elementType()== ElementType.VERTEX)){
                            this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                        }
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
                        if(!this.isLast() && (value.element.elementType()== ElementType.EDGE || value.element.elementType()== ElementType.VERTEX)){
                            this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                        }
                        el = value.element.copy();
                        el.setStorage(this);
                        el.createElement();
                    }
                    el.syncElement(value.element);
                    break;
                case RPC:
                    if(this.isLast() && value.element.elementType()==ElementType.FEATURE && ((Rpc)value.element).id.equals("434agg")){
                        System.out.println();
                    }
                    GraphElement rpcElement = this.getElement(value.element.getId(),value.element.elementType());
                    Rpc.execute(rpcElement, (Rpc) value.element);
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
