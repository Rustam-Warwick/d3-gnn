package partitioner;

import elements.GraphOp;
import functions.selectors.ElementForPartKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * HDRF but with Windowing the elements, wihtout watermark all will be evicted at the end of stream
 */
public class WindowedHDRF extends BasePartitioner{
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        HDRF hdrfMain = new HDRF();
        hdrfMain.partitions = this.partitions;
        DataStream<GraphOp> partitioned = hdrfMain.partition(inputDataStream, fineGrainedResourceManagementEnabled);
        SingleOutputStreamOperator<GraphOp> out = partitioned.keyBy(new ElementForPartKeySelector()).window(TumblingEventTimeWindows.of(Time.seconds(30))).process(new EmitAllFunction()).name(getName());
        if(fineGrainedResourceManagementEnabled){
            out.slotSharingGroup("gnn-"+out.getParallelism());
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

    public static class EmitAllFunction extends ProcessWindowFunction<GraphOp, GraphOp, String,TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<GraphOp, GraphOp, String, TimeWindow>.Context context, Iterable<GraphOp> elements, Collector<GraphOp> out) throws Exception {
            elements.forEach(out::collect);
        }
    }


}
