package partitioner;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

abstract public class BasePartitioner {
    /**
     * Number of partitions we should partition into
     */
    protected short partitions = -1;

    /**
     * Static Helper for getting the desired partitioner from its name
     */
    public static BasePartitioner getPartitioner(String name) {
        switch (name) {
            case "hypergraph-minmax":
                return new HyperGraphMinMax();
            case "hdrf":
                return new HDRF();
            case "windowed-hdrf":
                return new WindowedHDRF();
            case "windowed-random":
                return new WindowedRandom();
            case "buffered":
                return new BufferedPartitioner();
            default:
                return new RandomPartitioner();
        }
    }

    /**
     * Partition the incoming stream of {@link GraphOp}
     * <p>
     * A partitioned stream in this context is simply a stream of {@link GraphOp} with {@code partId} assigned
     * Partitioner is expected to gracefully handle out of ordered stream such as vertices or attached features arriving before the actual element.
     * In such cases it either cache/delay its partitioning or assign parts on the fly usually randomly
     * </p>
     *
     * @return partition stream
     */
    public abstract SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled);

    /**
     * Helper method for parsing partitioner specific command line arguments
     *
     * @param cmdArgs Array of parameter arguments passed
     * @return same partitioner
     */
    public BasePartitioner parseCmdArgs(String[] cmdArgs) {
        return this;
    }

    public final BasePartitioner setPartitions(short partitions) {
        this.partitions = partitions;
        return this;
    }
}
