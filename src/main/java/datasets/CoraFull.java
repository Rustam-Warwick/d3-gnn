package datasets;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import elements.DEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.CopyContext;
import elements.enums.ElementType;
import elements.enums.Op;
import features.Tensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CoraFull implements Dataset {
    private static final Pattern p = Pattern.compile("(?<name>\\d*\\.\\d*)");
    private final float trainSplitProbability = 0.4f;
    protected transient Path edgesFile;
    protected transient Path vertexFeatures;
    protected transient Path vertexLabels;

    public CoraFull(Path datasetPath) {
        vertexFeatures = Path.of(datasetPath.toString(), "vertex_features.npy");
        vertexLabels = Path.of(datasetPath.toString(), "vertex_labels.npy");
        edgesFile = Path.of(datasetPath.toString(), "edges");
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {

        return new KeyedProcessFunction<PartNumber, GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                assert value.element.elementType() == ElementType.EDGE;
                DEdge e = (DEdge) value.element;
//                if (e.src.getFeature("label") != null) {
//                    Feature<?, ?> label = e.src.getFeature("label"); // Get label
//                    e.src.features.removeIf(item -> "label".equals(item.getName()));
//
//                    float p = ThreadLocalRandom.current().nextFloat();
//                    if (p < trainSplitProbability) {
//                        label.setId("train_l");
//                    } else {
//                        label.setId("testLabel");
//                    }
//
//                    GraphOp copyGraphOp = value.copy();
//                    copyGraphOp.setElement(label);
////                    ctx.output(TRAIN_TEST_SPLIT_OUTPUT, copyGraphOp);
//                }
//                if (e.dest.getFeature("label") != null) {
//                    Feature<?, ?> label = e.dest.getFeature("label"); // Get label
//                    e.dest.features.removeIf(item -> "label".equals(item.getName())); // Remove it
//                    float p = ThreadLocalRandom.current().nextFloat();
//                    if (p < trainSplitProbability) {
//                        label.setId("train_l");
//                    } else {
//                        label.setId("testLabel");
//                    }
//                    GraphOp copyGraphOp = value.copy();
//                    copyGraphOp.setElement(label);
////                    ctx.output(TRAIN_TEST_SPLIT_OUTPUT, copyGraphOp); // Push to Side-Output
//                }
                GraphOp copy = value.copy();
                copy.setElement(value.element.copy(CopyContext.MEMENTO));
                ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, copy);
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
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        try {
//            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStream<String> edges = env.readTextFile(edgesFile.toString()).setParallelism(1);
//            DataStream<String> edges = env.fromSource(FileSource.forRecordStreamFormat(
//                    new TextLineInputFormat(), org.apache.flink.core.fs.Path.fromLocalFile(edgesFile.toFile())
//            ).build(), WatermarkStrategy.noWatermarks(), "file").setParallelism(1);

            DataStream<GraphOp> parsedEdges = edges.map(new EdgeParser()).setParallelism(1);
            DataStream<GraphOp> joinedData = parsedEdges
                    .flatMap(new JoinEdgeAndFeatures(this.vertexFeatures.toString(), this.vertexLabels.toString())).setParallelism(1)//Should be local
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<GraphOp>noWatermarks()
                            .withTimestampAssigner((event, ts) -> event.getTimestamp())).startNewChain();
            return joinedData;
        } catch (Exception e) {
            return null;
        }
    }

    protected static class EdgeParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            System.out.println(value);
            String[] edges = value.split(",");
            DEdge e = new DEdge(new Vertex(edges[0]), new Vertex(edges[1]));
            return new GraphOp(Op.COMMIT, e);
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
        public long timestamp;

        public JoinEdgeAndFeatures(String vertexFeaturesFile, String vertexLabelsFile) {
            this.vertexFeaturesFile = vertexFeaturesFile;
            this.vertexLabelsFile = vertexLabelsFile;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            FileInputStream vertexFeaturesIn = new FileInputStream(vertexFeaturesFile);
            FileInputStream vertexLabelsIn = new FileInputStream(vertexLabelsFile);
            this.vertexFeatures = NDHelper.decodeNumpy(BaseNDManager.getManager(), vertexFeaturesIn);
            this.vertexLabels = NDHelper.decodeNumpy(BaseNDManager.getManager(), vertexLabelsIn);
            this.seenVertices = new ArrayList<>(5000);
            this.timestamp = 0;
        }

        @Override
        public void flatMap(GraphOp value, Collector<GraphOp> out) throws Exception {
            assert value.element.elementType() == ElementType.EDGE; // No other thing is expected for this dataset
            timestamp++;
            DEdge dEdge = (DEdge) value.element;
            value.setTimestamp(timestamp);
            if (!seenVertices.contains(dEdge.getSrc().getId())) {
                int index = Integer.parseInt(dEdge.getSrc().getId());
                NDArray thisFeature = vertexFeatures.get(index);
                NDArray thisLabel = vertexLabels.get(index);
                dEdge.getSrc().setFeature("f", new Tensor(thisFeature));
                dEdge.getSrc().setFeature("label", new Tensor(thisLabel, true, (short) -1));
                seenVertices.add(dEdge.getSrc().getId());
            }
            if (!seenVertices.contains(dEdge.getDest().getId())) {
                int index = Integer.parseInt(dEdge.getDest().getId());
                NDArray thisFeature = vertexFeatures.get(index);
                NDArray thisLabel = vertexLabels.get(index);
                dEdge.getDest().setFeature("f", new Tensor(thisFeature));
                dEdge.getDest().setFeature("label", new Tensor(thisLabel, true, (short) -1));
                seenVertices.add(dEdge.getDest().getId());
            }
            out.collect(value);
        }
    }
}
