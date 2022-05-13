package functions.splitter;

import elements.ElementType;
import elements.Feature;
import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class EdgeTrainTestSplitter extends ProcessFunction<GraphOp, GraphOp> {
    public final double p;
    final OutputTag<GraphOp> trainOutput = new OutputTag<GraphOp>("training", TypeInformation.of(GraphOp.class)) {
    };

    public EdgeTrainTestSplitter() {
        this(0.005);
    }

    public EdgeTrainTestSplitter(double p) {
        this.p = p;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        double valueRandom = Math.random();
        if (value.element.elementType() == ElementType.EDGE && valueRandom < p) {
            value.element.setFeature("label", new Feature<Integer, Integer>(1));
            ctx.output(trainOutput, value);
            return;
        }
        out.collect(value);
    }
}
