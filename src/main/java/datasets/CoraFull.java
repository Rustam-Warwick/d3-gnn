package datasets;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import elements.*;
import features.Tensor;
import functions.nn.JavaTensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CoraFull implements Dataset{
    private static final Pattern p = Pattern.compile("(?<name>\\d*\\.\\d*)");
    private static final int dim = 8710;
    Path edgesFile;
    Path vertexFile;


    public CoraFull(Path edgesFile, Path vertexFile) {
        this.edgesFile = edgesFile;
        this.vertexFile = vertexFile;
    }
    private MapFunction<String, GraphOp> edgeMapper(){
        return value -> {
            String[] edges = value.split(",");
            Edge e = new Edge(new Vertex(edges[0]), new Vertex(edges[1]));
            return new GraphOp(Op.COMMIT, e, 0);
        };
    }
    private MapFunction<String, GraphOp> vertexMapper(){
        return new RichMapFunction<String, GraphOp>() {
            private transient NDManager manager;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                manager = NDManager.newBaseManager();
            }

            @Override
            public GraphOp map(String value) throws Exception {
                String[] vertexFeature = value.split(",\"");
                Vertex v = new Vertex(vertexFeature[0]);
                Matcher m = p.matcher(vertexFeature[1]);
                float[] arr = new float[dim];
                for(int i=0; m.find();i++){
                    float val = Float.valueOf(m.group(1));
                    arr[i] = val;
                }
                NDArray array = new JavaTensor(manager.create(arr));
                v.setFeature("feature", new Tensor(array));
                return new GraphOp(Op.COMMIT, v, 0);
            }
        };

    }

    private FlatMapFunction<GraphOp, GraphOp> joiner(){
        return new FlatMapFunction<GraphOp, GraphOp>() {
            private HashMap<String, GraphOp> pendingVertices = new HashMap<>();
            private long timestamp = 0;
            @Override
            public void flatMap(GraphOp value, Collector<GraphOp> out) throws Exception {
                if(value.element.elementType() == ElementType.VERTEX){
                    value.element.getFeature("feature").setTimestamp(++timestamp);
                    if(pendingVertices.containsKey(value.element.getId())){
                        out.collect(value);
                    }else{
                        pendingVertices.put(value.element.getId(), value);
                    }

                }else if(value.element.elementType() == ElementType.EDGE){
                    Edge e = (Edge)value.element;
                    value.element.setTimestamp(++timestamp);
                    out.collect(value);
                    if(pendingVertices.getOrDefault(e.src.getId(), null) != null){
                        out.collect(pendingVertices.get(e.src.getId()));
                    }
                    if(pendingVertices.getOrDefault(e.dest.getId(), null) != null){
                        out.collect(pendingVertices.get(e.dest.getId()));
                    }
                    pendingVertices.put(e.dest.getId(), null);
                    pendingVertices.put(e.src.getId(), null);
                }
            }
        };
    }
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        DataStream<String> edges = env.readTextFile(edgesFile.toString());
        DataStream<String> vertices = env.readTextFile(vertexFile.toString());
        DataStream<GraphOp> parsedEdges = edges.map(edgeMapper());
        DataStream<GraphOp> parsedVertices = vertices.map(vertexMapper());
        return parsedEdges.union(parsedVertices).flatMap(joiner()).setParallelism(1);
    }
}
