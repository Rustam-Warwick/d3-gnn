package partitioner;

import elements.GraphOp;
import functions.selectors.PartKeySelector;
import operators.FullBufferOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * HDRF but with Windowing the elements, wihtout watermark all will be evicted at the end of stream
 */
public class WindowedRandom extends BasePartitioner {
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        RandomPartitioner randomPartitioner = new RandomPartitioner();
        randomPartitioner.partitions = this.partitions;
        DataStream<GraphOp> partitioned = randomPartitioner.partition(inputDataStream, fineGrainedResourceManagementEnabled);
        SingleOutputStreamOperator<GraphOp> out = partitioned
                .keyBy(new PartKeySelector())
                .transform("Random+Window", TypeInformation.of(GraphOp.class), new FullBufferOperator<GraphOp>())
                .name("Random+Window");
        if (fineGrainedResourceManagementEnabled) {
            out.slotSharingGroup("gnn-0");
        }
        return out;
    }

}
