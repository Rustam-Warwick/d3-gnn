package partitioner;

import elements.GraphOp;
import org.apache.flink.api.common.functions.RichMapFunction;

abstract public class BasePartitioner extends RichMapFunction<GraphOp, GraphOp> {
    public short partitions = -1;
    abstract public boolean isParallel();
}
