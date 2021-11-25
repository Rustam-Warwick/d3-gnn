package partitioner;

import edge.BaseEdge;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import types.GraphQuery;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPartitioning extends BasePartitioner {
    public final int partitions = 2;
    public final HashMap<String,Short> masters = new HashMap<>();

    @Override
    public GraphQuery map(GraphQuery value) throws Exception {
        if(Objects.isNull(value.part)){
            value.part = (short) ThreadLocalRandom.current().nextInt(0,this.partitions);
        }

        if(value.element instanceof BaseEdge){
            BaseEdge tmp = (BaseEdge) value.element;
            masters.putIfAbsent(tmp.source.getId(),value.part);
            masters.putIfAbsent(tmp.destination.getId(),value.part);
            if(masters.get(tmp.source.getId()).equals(value.part))tmp.source.setMasterPart(null);
            else tmp.source.setMasterPart(masters.get(tmp.source.getId()));
            if(masters.get(tmp.destination.getId()).equals(value.part))tmp.destination.setMasterPart(null);
            else tmp.destination.setMasterPart(masters.get(tmp.destination.getId()));
//            System.out.format("Sending vertex %s to %s\n",tmp.source.getId(),tmp.source.getState());
        }

        return value;
    }
}
