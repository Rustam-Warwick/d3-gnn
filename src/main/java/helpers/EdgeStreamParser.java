package helpers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.Edge;
import elements.GraphOp;
import elements.Op;
import elements.Vertex;
import features.TString;
import features.Tensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;

public class EdgeStreamParser extends RichMapFunction<String, GraphOp> {
    public final HashMap<String, NDArray> oneHotFeatures = new HashMap<>();
    public final String[] categories;

    public EdgeStreamParser(String[] categories){
        this.categories = categories;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        NDManager manager = NDManager.newBaseManager();
        NDArray tmp = manager.eye(this.categories.length);
        for(int i=0; i<this.categories.length;i++){
            this.oneHotFeatures.put(this.categories[i], tmp.get(i));
        }
    }
    @Override
    public GraphOp map(String value) throws Exception {
        String[] res = value.split(",");
        GraphOp tmp;
        try{
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            Vertex src = new Vertex(res[0]);
            Vertex dest = new Vertex(res[1]);
            Edge edge = new Edge(src,dest);
            tmp = new GraphOp(Op.COMMIT, edge);
        }catch (Exception e){
            String sourceId = res[0];
            Vertex vrt = new Vertex(sourceId);
            vrt.setFeature("label",new Tensor(this.oneHotFeatures.get(res[1])));
            tmp = new GraphOp(Op.COMMIT, vrt);
        }
        return tmp;
    }
}
