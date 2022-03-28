package elements;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.List;

/**
 * Plugin is a unique Graph element that is attached to storage, so it is not in the life cycle of logical keys
 */
public class Plugin extends ReplicableGraphElement{
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
        return this.storage.otherKeys;
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
    }

    /**
     * Callback when this plugin is first added to the storage
     */
    public void add(){

    }

}
