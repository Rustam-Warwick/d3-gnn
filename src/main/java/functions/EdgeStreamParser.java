package functions;

import elements.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.List;

public class EdgeStreamParser extends RichMapFunction<String, GraphOp> {
    public final List<String> categories;
    public final String delimiter;

    public EdgeStreamParser(String[] categories) {
        this.categories = List.of(categories);
        this.delimiter = ",";
    }

    public EdgeStreamParser(String[] categories, String delimiter) {
        this.categories = List.of(categories);
        this.delimiter = delimiter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public GraphOp map(String value) throws Exception {
        String[] res = value.split(this.delimiter);
        GraphOp tmp;
        try {
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            Vertex src = new Vertex(res[0]);
            Vertex dest = new Vertex(res[1]);
            Edge edge = new Edge(src, dest);
            tmp = new GraphOp(Op.COMMIT, edge);
        } catch (Exception e) {
            String sourceId = res[0];
            Vertex vrt = new Vertex(sourceId);
            int shallowLabel = this.categories.indexOf(res[1]);
            vrt.setFeature("label", new Feature<Integer, Integer>(shallowLabel));
            tmp = new GraphOp(Op.COMMIT, vrt);
        }
        return tmp;
    }
}
