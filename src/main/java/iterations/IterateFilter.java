package iterations;

import elements.GraphOp;
import elements.Op;
import elements.ReplicaState;
import org.apache.flink.api.common.functions.FilterFunction;

public class IterateFilter implements FilterFunction<GraphOp> {
    @Override
    public boolean filter(GraphOp value) throws Exception {
//        if(value.element.getId().equals("434") && value.op == Op.SYNC && value.element.state()== ReplicaState.MASTER){
//            System.out.println("Sync Request From Master: "+value.element.partId + "To Replica: "+value.part_id);
//        }
//        else if(value.element.getId().equals("434") && value.op == Op.SYNC && value.element.state()== ReplicaState.REPLICA){
//            System.out.println("Sync Request From Replica: "+value.element.partId + "To Master: "+value.part_id);
//        }
        return value.state == IterationState.ITERATE;
    }
}
