package functions;

import elements.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MovieLensStreamParser extends RichFlatMapFunction<String, GraphOp> {
    public final String delimiter;

    public MovieLensStreamParser() {
        delimiter = ",";
    }

    public MovieLensStreamParser(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void flatMap(String value, Collector<GraphOp> out) throws Exception {
        String[] res = value.split(this.delimiter);
        GraphOp tmp;
        try {
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            float rating = Float.valueOf(res[2]);
            Vertex src = new Vertex(res[0]);
            Vertex dest = new Vertex(res[1]);
            Edge edge = new Edge(src, dest);
//            edge.setFeature("rating", new Feature<Float, Float>(rating));
            Edge edgeReverse = (Edge) edge.deepCopy();
            edgeReverse.reverse();
            out.collect(new GraphOp(Op.COMMIT, edge));
            out.collect(new GraphOp(Op.COMMIT, edgeReverse));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
