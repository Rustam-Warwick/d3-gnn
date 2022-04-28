package functions.parser;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.Edge;
import elements.GraphOp;
import elements.Op;
import elements.Vertex;
import features.VTensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class CoraMergedStreamParser extends RichFlatMapFunction<String, GraphOp> {
    public final String delimiter;
    public final List<String> categories;
    public List<NDArray> categoriesFeatures;

    public CoraMergedStreamParser() {
        this(",", "Case_Based", "Rule_Learning", "Neural_Networks", "Theory", "Genetic_Algorithms", "Reinforcement_Learning", "Probabilistic_Methods");
    }

    public CoraMergedStreamParser(String delimiter, String... categories) {
        this.delimiter = delimiter;
        this.categories = List.of(categories);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.categoriesFeatures = new ArrayList<>();
        NDManager m = NDManager.newBaseManager();
        NDArray tmpEye = m.eye(this.categories.size());
        for (int i = -1; i < this.categories.size(); i++) {
            categoriesFeatures.add(tmpEye.get(i));
        }
    }

    @Override
    public void flatMap(String value, Collector<GraphOp> out) throws Exception {
        String[] res = value.split(this.delimiter);
        try {
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            Vertex src = new Vertex(res[0]);
            Vertex dest = new Vertex(res[1]);
            Edge edge = new Edge(src, dest);
            out.collect(new GraphOp(Op.COMMIT, edge));
        } catch (Exception e) {
            String sourceId = res[0];
            Vertex vrt = new Vertex(sourceId);
            int index = this.categories.indexOf(res[1]);
            vrt.setFeature("feature", new VTensor(new Tuple2<>(categoriesFeatures.get(index), 0)));
            out.collect(new GraphOp(Op.COMMIT, vrt));
        }
    }

}
