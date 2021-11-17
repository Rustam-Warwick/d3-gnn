package features;

import com.twitter.chill.java.ClosureSerializer;
import types.GraphElement;
import types.SerialFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ReplicableArrayListFeature<T> extends ReplicableFeature<ArrayList<T>> {

    public ReplicableArrayListFeature() {
        super();
    }

    public ReplicableArrayListFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableArrayListFeature(String fieldName, GraphElement element) {
        super(fieldName, element,new ArrayList<>());
    }

    public ReplicableArrayListFeature(String fieldName, GraphElement element, ArrayList<T> value) {
        super(fieldName, element, value);
    }

    @Override
    public void startTimer(int period,String ...ids){
        if (this.attachedId == null || !Arrays.asList(ids).contains(this.attachedId))  return;
        Timer a = new Timer();
        ReplicableArrayListFeature<T> as = this;
        a.schedule(new TimerTask() {
            @Override
            public void run() {
                StringBuilder values = new StringBuilder();
                for(T a: as.value) values.append(a.toString()+" ");
                System.out.format("Part %s  Size:%s Completed:%s Values:%s ReplicationState:%s Updated last:%s \n",as.partId,as.value.size(),as.fuzzyValue.isDone(),values,as.state,as.lastModified);
            }
        },0,period);
    }

    public void add(T elem){
        SerialFunction<ReplicableFeature<ArrayList<T>>,Boolean> fn = item->{
            if(!item.value.contains(elem)){
                item.value.add(elem);
                return true;
            }
            return false;
        };
        this.editHandler(fn);
    }

}
