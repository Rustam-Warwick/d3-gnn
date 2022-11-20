package partitioner;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import picocli.CommandLine;

import javax.annotation.Nullable;

abstract public class BasePartitioner {

    public BasePartitioner(String[] cmdArgs){
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Number of partitions we should partition into
     */
    protected short partitions = -1;

    /**
     * Static Helper for getting the desired partitioner from its name
     */
    @Nullable
    public static BasePartitioner getPartitioner(String name, String[] cmdArgs) {
        switch (name) {
            case "hypergraph-minmax":
                return new HyperGraphMinMax(cmdArgs);
            case "hdrf":
                return new HDRF(cmdArgs);
            case "random":
                return new RandomPartitioner(cmdArgs);
            default:
                return null;
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
    public abstract SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream);

    public final BasePartitioner setPartitions(short partitions) {
        this.partitions = partitions;
        return this;
    }
}
