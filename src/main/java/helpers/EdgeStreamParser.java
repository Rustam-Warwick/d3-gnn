package helpers;

import elements.Edge;
import elements.GraphOp;
import elements.Op;
import elements.Vertex;
import org.apache.flink.api.common.functions.MapFunction;

public class EdgeStreamParser implements MapFunction<String, GraphOp> {

    @Override
    public GraphOp map(String value) throws Exception {
        String[] res = value.split("\t");
        GraphOp tmp = null;
        try{
            Integer.valueOf(res[0]);
            Integer.valueOf(res[1]);
            Vertex src = new Vertex(res[0]);
            Vertex dest = new Vertex(res[1]);
            Edge edge = new Edge(src,dest);
            tmp = new GraphOp(Op.COMMIT, edge);
        }catch (Exception e){

        }

        return tmp;
    }
}
