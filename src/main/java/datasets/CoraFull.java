package datasets;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDSerializer;
import elements.*;
import features.Tensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CoraFull implements Dataset {
    private static final Pattern p = Pattern.compile("(?<name>\\d*\\.\\d*)");
    protected Path edgesFile;
    protected Path vertexFeatures;
    protected Path vertexLabels;

    public CoraFull(Path datasetPath) {
        vertexFeatures = Path.of(datasetPath.toString(), "vertex_features.npy");
        vertexLabels = Path.of(datasetPath.toString(), "vertex_labels.npy");
        edgesFile = Path.of(datasetPath.toString(), "edges");
    }

    @Override
    public ProcessFunction<GraphOp, GraphOp> trainTestSplitter() {
        return new ProcessFunction<GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                assert value.element.elementType() == ElementType.EDGE;
                Edge e = (Edge) value.element;
                if(e.src.getFeature("label")!=null){
                    Feature<?, ?> label = e.src.getFeature("label"); // Get label
                    e.src.features.removeIf(item -> "label".equals(item.getName()));
                    label.setId("testLabel");
                    GraphOp copyGraphOp = value.copy();
                    copyGraphOp.setElement(label);
                    ctx.output(TRAIN_TEST_DATA_OUTPUT, copyGraphOp);
                }
                if(e.dest.getFeature("label")!=null){
                    Feature<?, ?> label = e.dest.getFeature("label"); // Get label
                    e.dest.features.removeIf(item -> "label".equals(item.getName())); // Remove it
                    label.setId("testLabel"); // Change name
                    GraphOp copyGraphOp = value.copy();
                    copyGraphOp.setElement(label);
                    ctx.output(TRAIN_TEST_DATA_OUTPUT, copyGraphOp); // Push to Side-Output
                }
                out.collect(value);
            }
        };
    }

    /**
     * Side Output Contains the train test splitted data
     *
     * @implNote testLabel Feature is the testLabel
     * @implNote trainLabel Feature is the trainlabel
     */
    @Override
    public DataStream<GraphOp>[] build(StreamExecutionEnvironment env) {
        try {
            DataStream<String> edges = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(edgesFile.toString())), edgesFile.toString(), FileProcessingMode.PROCESS_CONTINUOUSLY, 10000).setParallelism(1);
            DataStream<GraphOp> parsedEdges = edges.map(new EdgeParser()).setParallelism(1);
            DataStream<GraphOp> joinedData = parsedEdges
                    .flatMap(new JoinEdgeAndFeatures(this.vertexFeatures.toString(), this.vertexLabels.toString())).setParallelism(1)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<GraphOp>forBoundedOutOfOrderness(Duration.ofMillis(0))
                            .withTimestampAssigner((event, ts) -> event.getTimestamp()));
            return new DataStream[]{joinedData};


        } catch (Exception e) {
            return null;
        }
    }

    protected static class EdgeParser implements MapFunction<String, GraphOp>{
        @Override
        public GraphOp map(String value) throws Exception {
            String[] edges = value.split(",");
            Edge e = new Edge(new Vertex(edges[0]), new Vertex(edges[1]));
            return new GraphOp(Op.COMMIT, e, 0);
        }
    }

    /**
     * Joiner RichFlatMapFunction
     */
    protected static class JoinEdgeAndFeatures extends RichFlatMapFunction<GraphOp, GraphOp> {
        public String vertexFeaturesFile;
        public String vertexLabelsFile;
        public transient NDArray vertexFeatures;
        public transient NDArray vertexLabels;

        public List<String> seenVertices;
        public int timestamp;

        public JoinEdgeAndFeatures(String vertexFeaturesFile, String vertexLabelsFile) {
            this.vertexFeaturesFile = vertexFeaturesFile;
            this.vertexLabelsFile = vertexLabelsFile;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            FileInputStream vertexFeaturesIn = new FileInputStream(vertexFeaturesFile);
            FileInputStream vertexLabelsIn = new FileInputStream(vertexLabelsFile);
            this.vertexFeatures =  NDSerializer.decodeNumpy(BaseNDManager.threadNDManager.get(), vertexFeaturesIn);
            this.vertexLabels = NDSerializer.decodeNumpy(BaseNDManager.threadNDManager.get(), vertexLabelsIn);
            this.seenVertices = new ArrayList<>();
            this.timestamp = 0;

        }

        @Override
        public void flatMap(GraphOp value, Collector<GraphOp> out) throws Exception {
            assert value.element.elementType() == ElementType.EDGE; // No other thing is expected for this dataset
            timestamp++;
            Edge edge = (Edge) value.element;
            edge.setTimestamp(timestamp);
            value.setTimestamp(timestamp);
            if (!seenVertices.contains(edge.src.getId())) {
                int index = Integer.parseInt(edge.src.getId());
                NDArray thisFeature = vertexFeatures.get(index);
                NDArray thisLabel = vertexLabels.get(index);
                edge.src.setFeature("feature", new Tensor(thisFeature));
                edge.src.setFeature("label", new Tensor(null, thisLabel, true, (short) -1));
                seenVertices.add(edge.src.getId());
            }
            if (!seenVertices.contains(edge.dest.getId())) {
                int index = Integer.parseInt(edge.dest.getId());
                NDArray thisFeature = vertexFeatures.get(index);
                NDArray thisLabel = vertexLabels.get(index);
                edge.dest.setFeature("feature", new Tensor(thisFeature));
                edge.dest.setFeature("label", new Tensor(null, thisLabel, true, (short) -1));
                seenVertices.add(edge.dest.getId());
            }
            out.collect(value);
        }
    }
}
