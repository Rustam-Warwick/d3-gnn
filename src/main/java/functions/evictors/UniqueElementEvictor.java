package functions.evictors;

import elements.GraphElement;
import elements.GraphOp;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Evictor that makes sure that only one GraphElement is in the window
 */
public class UniqueElementEvictor implements Evictor<GraphOp, TimeWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<GraphOp>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        HashMap<String, Integer> idCount = new HashMap<>();
        for(TimestampedValue<GraphOp> op: elements){
            if(idCount.containsKey(op.getValue().element.getId())){
                idCount.compute(op.getValue().element.getId(), (key,value)->value + 1);
            }else{
                idCount.put(op.getValue().element.getId(), 1);
            }
        }
        Iterator<TimestampedValue<GraphOp>> iterator = elements.iterator();
        while(iterator.hasNext()){
            GraphElement el = iterator.next().getValue().element;
            int count = idCount.get(el.getId());
            if(count > 1){
                idCount.compute(el.getId(), (key,value)->value - 1);
                iterator.remove();
            }
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<GraphOp>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

    }
}
