package functions.splitter;

import elements.ElementType;
import elements.GraphOp;
import features.VTensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

public class FeaturedVertexSplitter extends ProcessFunction<GraphOp, GraphOp> {
    public final double p;
    final OutputTag<GraphOp> trainOutput = new OutputTag<>("training", TypeInformation.of(GraphOp.class)) {
    };

    public FeaturedVertexSplitter() {
        this(0.015);
    }

    public FeaturedVertexSplitter(double p) {
        this.p = p;
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        if(value.element.elementType() == ElementType.VERTEX) {
            double valueRandom = Math.random();
            if (valueRandom < p) {
                VTensor feature = (VTensor) value.element.getFeature("feature");
                value.element.features.remove(feature);
                value.element.setFeature("label", feature);
                ctx.output(trainOutput, value);
                value.element.features.remove(feature);
                value.element.setFeature("feature", new VTensor(new Tuple2<>(feature.getValue().zerosLike(), 0)));
            }
        }
        out.collect(value);
    }
}

