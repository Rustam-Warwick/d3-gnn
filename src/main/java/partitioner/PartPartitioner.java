package partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class PartPartitioner implements Partitioner<Short> {


    @Override
    public int partition(Short key, int numPartitions) {
        return key % numPartitions;
    }

}
