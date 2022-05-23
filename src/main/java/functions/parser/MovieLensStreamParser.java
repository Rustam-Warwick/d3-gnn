package functions.parser;

import elements.Edge;
import elements.GraphOp;
import elements.Op;
import elements.Vertex;
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
        try {
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            long timestamp = Integer.parseInt(res[3]);
            Vertex src = new Vertex(res[0]);
            src.setTimestamp(timestamp);
            Vertex dest = new Vertex(res[1]);
            dest.setTimestamp(timestamp);
            Edge edge = new Edge(src, dest);
            edge.setTimestamp(timestamp); // Added timestamps
            Edge edgeReverse = edge.deepCopy().reverse();
            out.collect(new GraphOp(Op.COMMIT, edge, edge.getTimestamp()));
            out.collect(new GraphOp(Op.COMMIT, edgeReverse, edge.getTimestamp()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
