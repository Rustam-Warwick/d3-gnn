package partitioner;

import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

import java.util.ServiceLoader;

/**
 * Abstract class representing all Streaming Graph Partitioners
 * Follows {@link ServiceLoader} pattern and can be extended
 */
abstract public class Partitioner {

    /**
     * Has no effect just make sure one command var exists in Partitioner to not throw error
     */
    @CommandLine.Option(names = {"--dataset:processOnce"}, defaultValue = "true", fallbackValue = "true", arity = "1", description = "Dataset: Process once or continuously")
    protected boolean processOnce;

    /**
     * Number of logical parts the partitioner sees, usually set to {@code env.getMaxParallelism()}
     */
    protected short partitions = -1;

    /**
     * Static Helper for getting the desired partitioner from its name
     */
    @Nullable
    public static Partitioner getPartitioner(String name, String[] cmdArgs) {
        ServiceLoader<Partitioner> partitionerServiceLoader = ServiceLoader.load(Partitioner.class);
        for (Partitioner partitioner : partitionerServiceLoader) {
            if (partitioner.isResponsibleFor(name)) {
                partitioner.parseCmdArgs(cmdArgs);
                return partitioner;
            }
        }
        return null;
    }

    /**
     * Partition the incoming stream of {@link GraphOp}
     * <p>
     * A partitioned stream in this context is simply a stream of {@link GraphOp} with {@code partId} assigned
     * Partitioner is expected to gracefully handle out of ordered stream such as vertices or attached features arriving before the actual element.
     * In such cases it either cache/delay its partitioning or assign parts on the fly usually randomly
     * </p>
     *
     * @return partition {@link DataStream}
     */
    public abstract SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream);

    /**
     * Process command line arguments
     */
    public final void parseCmdArgs(String[] cmdArgs) {
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Return true if this partitioner has the given name
     */
    public abstract boolean isResponsibleFor(String partitionerName);

    /**
     * Set number of logical parts in this partitioner
     */
    public final Partitioner setPartitions(short partitions) {
        this.partitions = partitions;
        return this;
    }
}
