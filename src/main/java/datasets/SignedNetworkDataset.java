package datasets;

import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.*;
import features.Tensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Dataset for signed network where format is assumed to <src, dest, sign ,Optional<ts>>
 */
public class SignedNetworkDataset implements Dataset {

    public final String fileName;

    public SignedNetworkDataset(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        DataStream<GraphOp> out;
        SingleOutputStreamOperator<String> fileReader = env.readTextFile(fileName).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = fileReader.map(new Parser()).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> timestampExtracted = parsed.assignTimestampsAndWatermarks(WatermarkStrategy.<GraphOp>noWatermarks().withTimestampAssigner(new SerializableTimestampAssigner<GraphOp>() {
            @Override
            public long extractTimestamp(GraphOp element, long recordTimestamp) {
                Long ts = element.getTimestamp();
                element.setTimestamp(null);
                return ts == null ? Long.MIN_VALUE : ts;
            }
        })).setParallelism(1);


        if (fineGrainedResourceManagementEnabled) {
            fileReader.slotSharingGroup("file-input");
            parsed.slotSharingGroup("file-input");
            timestampExtracted.slotSharingGroup("file-input");
        }
        return timestampExtracted;
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                assert value.getElement().elementType() == ElementType.EDGE && value.getElement().features != null;
                UniEdge e = (UniEdge) value.getElement();
                if (ThreadLocalRandom.current().nextFloat() < 0.5) {
                    // Train
                    e.getFeature("sign").setName("train_l");
                } else {
                    // Test
                    e.getFeature("sign").setName("testLabel");
                }
                ctx.output(TRAIN_TEST_SPLIT_OUTPUT, value);
                GraphOp topologyGraphOp = value.shallowCopy();
                topologyGraphOp.setElement(e.copy());
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, topologyGraphOp);
                out.collect(topologyGraphOp);
            }
        };
    }

    public static class Parser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            UniEdge e = new UniEdge(new Vertex(values[0]), new Vertex(values[1]));
            e.setFeature("sign", new Tensor(LifeCycleNDManager.getInstance().create(1)));
            return new GraphOp(Op.COMMIT, e, Float.valueOf(values[3]).longValue());
        }
    }

}
