package helpers;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyKeyGroupPartitioner extends KeyGroupStreamPartitioner<GraphOp, Short> {
    private final KeySelector<GraphOp, Short> keySelector;
    public MyKeyGroupPartitioner(KeySelector<GraphOp, Short> keySelector, int maxParallelism) {
        super(keySelector, maxParallelism);
        this.keySelector = keySelector;
    }
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<GraphOp>> record) {
        Short key;
        try {
            key = this.keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }
        return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                getMaxParallelism(),
                numberOfChannels,
                KeyGroupRangeAssignment.computeKeyGroupForKeyHash(key, getMaxParallelism())
                );
    }


}
