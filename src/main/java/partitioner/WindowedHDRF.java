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
public class WindowedHDRF extends BasePartitioner {
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        HDRF hdrfMain = new HDRF();
        hdrfMain.partitions = this.partitions;
        DataStream<GraphOp> partitioned = hdrfMain.partition(inputDataStream, fineGrainedResourceManagementEnabled);
        SingleOutputStreamOperator<GraphOp> out = partitioned
                .keyBy(new PartKeySelector())
                .transform(getName(), TypeInformation.of(GraphOp.class), new FullBufferOperator<GraphOp>())
                .name(getName());
        if (fineGrainedResourceManagementEnabled) {
            out.slotSharingGroup("gnn-0");
        }
        return out;
    }

    @Override
    public void parseCmdArgs(String[] cmdArgs) {

    }

    @Override
    public String getName() {
        return "hdrf-windowed";
    }

}
