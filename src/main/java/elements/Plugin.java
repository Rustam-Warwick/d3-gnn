package elements;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import state.KeyGroupRangeAssignment;

import java.util.*;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement{
    public List<Short> replicaList = null;
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
    public List<Short> replicaParts() {
        return this.replicaList;
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
        this.replicaList = new ArrayList<>();
        int[] seen = new int[this.storage.parallelism];
        Arrays.fill(seen, -1);
        for(short i=0;i<this.storage.getRuntimeContext().getMaxNumberOfParallelSubtasks();i++){
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), this.storage.maxParallelism, this.storage.parallelism);
            if(seen[operatorIndex] == -1){
                seen[operatorIndex] = i;
            }
        }
        this.master = (short) Arrays.stream(seen).filter(item->item!=-1).findFirst().getAsInt(); // Master is the key closes to zero's parallel subtask
        this.partId = (short) seen[this.storage.operatorIndex]; // Part id of plugin is the first occurance of its key
        if(this.state() == ReplicaState.MASTER){
            for(int i=0; i< this.storage.parallelism; i++){
                if(seen[i] == -1 || seen[i] == this.partId)continue;
                this.replicaList.add((short) seen[i]);
            }
        }
    }

    /**
     * Callback when this plugin is first added to the storage
     */
    public void add(){

    }

}
