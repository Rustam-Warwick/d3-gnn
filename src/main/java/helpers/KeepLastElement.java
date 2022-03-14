package helpers;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;
import java.util.Objects;

public class KeepLastElement implements Evictor<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>, TimeWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        int firstCount = 0;
        int secondCount = 0;
        for(TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>> el: elements){
            if(Objects.nonNull(el.getValue().getOne()))firstCount++;
            else if(Objects.nonNull(el.getValue().getTwo()))secondCount++;
        }

        for (Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>> el = iterator.next();
            if(Objects.nonNull(el.getValue().getOne()) && firstCount > 1){
                firstCount--;
                iterator.remove();
            }else if(Objects.nonNull(el.getValue().getTwo()) && secondCount > 1){
                secondCount--;
                iterator.remove();
            }
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

    }
}
