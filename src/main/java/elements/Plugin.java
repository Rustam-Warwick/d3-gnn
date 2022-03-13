package elements;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Plugin extends ReplicableGraphElement{
    public List<Short> replicaList=null;
    public Plugin(){
        super(null, false, (short) 0);
    }
    public Plugin(String id) {
        super(id, false, (short) 0);
    }

    @Override
    public Boolean createElement() {
        return false;
    }

    @Override
    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement) {
        return null;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        return null;
    }

    @Override
    public short getPartId() {
        if(Objects.nonNull(this.storage))return this.storage.currentKey;
        return super.getPartId();
    }

    @Override
    public List<Short> replicaParts() {
        return this.replicaList;
    }

    @Override
    public short masterPart() {
        return 0;
    }

    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }

    public void addElementCallback(GraphElement element){

    }
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement){

    }
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out){

    }

    public void close(){

    }

    public void open(){
        if(Objects.isNull(this.replicaList)){
            this.replicaList = new ArrayList<>();
            for(short i=1;i<this.storage.getRuntimeContext().getMaxNumberOfParallelSubtasks();i++){
                replicaList.add(i);
            }
        }
    }

}
