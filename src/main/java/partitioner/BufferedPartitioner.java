package partitioner;

import elements.GraphOp;
import functions.selectors.PartKeySelector;
import operators.FullBufferOperator;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * HDRF but with Windowing the elements, wihtout watermark all will be evicted at the end of stream
 */
public class BufferedPartitioner extends BasePartitioner {
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        HDRF mainPartitioner = (HDRF) new HDRF().setPartitions(partitions);
        DataStream<GraphOp> partitioned = mainPartitioner.partition(inputDataStream, fineGrainedResourceManagementEnabled);
        SingleOutputStreamOperator<GraphOp> out = partitioned
                .keyBy(new PartKeySelector())
                .transform("HDRF+Window", TypeInformation.of(GraphOp.class), new FullBufferOperator<GraphOp>())
                .name("HDRF+Window");
        if (fineGrainedResourceManagementEnabled) {
            out.slotSharingGroup("gnn-0");
        }
        return out;
    }

    @Override
    public BasePartitioner parseCmdArgs(String[] cmdArgs) {
        Option actual_partitioner = Option
                .builder()
                .required(true)
                .desc("Actual Partitioner for buffering")
                .type(Boolean.class)
                .longOpt("actualPartitioner")
                .build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        options.addOption(actual_partitioner);
        try {
            CommandLine commandLine = parser.parse(options, cmdArgs);
        } catch (Exception e) {
            throw new IllegalStateException("Need to specify actualPartitioner when using BufferedPartitioner");
        }
        return super.parseCmdArgs(cmdArgs);
    }
}
