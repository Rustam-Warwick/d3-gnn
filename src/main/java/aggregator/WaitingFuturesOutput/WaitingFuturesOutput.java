package aggregator.WaitingFuturesOutput;

import aggregator.BaseAggregator;
import part.BasePart;
import types.GraphQuery;
import vertex.BaseVertex;

import java.util.Timer;
import java.util.TimerTask;

public class WaitingFuturesOutput<VT extends BaseVertex> extends BaseAggregator<VT> {
    public transient Timer t;



    @Override
    public boolean shouldTrigger(GraphQuery o) {
        return true;
    }

    @Override
    public void dispatch(GraphQuery msg) {
       if(t==null){
           this.t = new Timer();
           WaitingFuturesOutput<VT> ref = this;
           this.t.schedule(new TimerTask() {
               @Override
               public void run() {
                    long a = ref.part.storage.getVertices().filter(item->item.parts!=null && !item.parts.fuzzyValue.isDone()).count();
                    System.out.format("Total %s vertices are waiting in part %s\n",a,part.getPartId());
               }
           }, 0,1000);
       }
    }
}
