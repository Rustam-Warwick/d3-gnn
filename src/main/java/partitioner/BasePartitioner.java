package partitioner;

import elements.GraphOp;
import org.apache.flink.api.common.functions.RichMapFunction;

abstract public class BasePartitioner extends RichMapFunction<GraphOp, GraphOp> {
    /**
     * Number of partitions we should partition into
     */
    public short partitions = -1;

    /**
     * Can we use more than 1 parallelism with this RichMapFunction
     *
     * @return Can we?
     */
    abstract public boolean isParallel();
}
