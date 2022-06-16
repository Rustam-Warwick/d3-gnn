package partitioner;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

abstract public class BasePartitioner {
    /**
     * Number of partitions we should partition into
     */
    public short partitions = -1;

    /**
     * Static Helper for getting the desired partitioner from its name
     */
    public static BasePartitioner getPartitioner(String name) {
        switch (name) {
            case "hdrf":
                return new HDRF();
            default:
                return new RandomPartitioner();
        }
    }

    public abstract SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream);

    /**
     * Name of this partitioner
     */
    abstract public String getName();
}
