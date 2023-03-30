package partitioner;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.FullBufferOperator;

/**
 * Partitioner that wraps and buffers the result of any other partitioner
 * Expects name to be of format <strong>{name_of_partitioner}:buffered</strong>
 */
public class BufferedPartitioner extends Partitioner {

    protected String mainPartitionerName;

    protected Partitioner mainPartitioner;

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        return mainPartitioner.partition(inputDataStream).transform("Buffer", TypeInformation.of(GraphOp.class), new FullBufferOperator<>()).setParallelism(1);
    }

    @Override
    public void parseCmdArgs(String[] cmdArgs) {
        super.parseCmdArgs(cmdArgs);
        mainPartitioner = Partitioner.getPartitioner(mainPartitionerName, cmdArgs);
    }

    @Override
    public Partitioner setPartitions(short partitions) {
        mainPartitioner.setPartitions(partitions);
        return super.setPartitions(partitions);
    }

    @Override
    public boolean isResponsibleFor(String partitionerName) {
        boolean isResp = partitionerName.contains(":buffered");
        if(isResp) mainPartitionerName = partitionerName.split(":buffered")[0];
        return isResp;
    }
}
