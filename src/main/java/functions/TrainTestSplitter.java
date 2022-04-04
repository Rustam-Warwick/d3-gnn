package functions;

import elements.GraphOp;
import org.apache.flink.api.common.functions.MapFunction;
import scala.Tuple2;

public class TrainTestSplitter implements MapFunction<GraphOp, Tuple2<GraphOp, Boolean>> {
    public final double p;

    public TrainTestSplitter() {
        p = 0.002d;
    }

    public TrainTestSplitter(double p) {
        this.p = p;
    }

    @Override
    public Tuple2<GraphOp, Boolean> map(GraphOp value) throws Exception {
        double valuerandom = Math.random();
        boolean isTraining = false;
        if (valuerandom < p) {
            isTraining = true;
        }
        return new Tuple2<>(value, isTraining);
    }
}
