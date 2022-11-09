package datasets;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.*;
import elements.enums.Op;
import features.Tensor;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class Reddit implements Dataset {

    /**
     * {@inheritDoc}
     */
    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env, boolean fineGrainedResourceManagementEnabled) {
        String baseDirectory = Path.of(System.getenv("DATASET_DIR")).toString();
        String fileName = Path.of(baseDirectory, "reddit", "graph.tsv").toString();
        String labelFileName = Path.of(baseDirectory, "reddit", "node_labels.npy").toString();
        String featureFileName = Path.of(baseDirectory, "reddit", "node_features.npy").toString();
        SingleOutputStreamOperator<String> fileReader = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(fileName)), fileName, FileProcessingMode.PROCESS_ONCE, 0).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> parsed = null;
        try {
            parsed = fileReader.process(new Parser(labelFileName, featureFileName)).name("Parser").setParallelism(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (fineGrainedResourceManagementEnabled) {
            // All belong to the same slot sharing group
            fileReader.slotSharingGroup("file-input");
            env.registerSlotSharingGroup(SlotSharingGroup.newBuilder("parser").setCpuCores(1).setTaskOffHeapMemoryMB(760).setTaskHeapMemoryMB(100).build());
            parsed.slotSharingGroup("parser");
        }
        return parsed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> trainTestSplitter() {
        return new Splitter();
    }

    public static class Splitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {

        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            DEdge incomingDEdge = (DEdge) value.getElement();
            value.setElement(new DEdge(incomingDEdge.getSrc().copy(), incomingDEdge.getDest().copy()));
            out.collect(value);
            ctx.output(TOPOLOGY_ONLY_DATA_OUTPUT, value);
            if (incomingDEdge.getSrc().containsFeature("f")) {
                Feature<?, ?> feature = incomingDEdge.getSrc().getFeature("f");
                feature.element = null;
                Vertex tmpSrc = incomingDEdge.getSrc().copy();
                tmpSrc.setFeature("f", feature);
                out.collect(new GraphOp(Op.COMMIT, tmpSrc.masterPart(), tmpSrc));
            }
            if (incomingDEdge.getSrc().containsFeature("train_l")) {
                Feature<?, ?> feature = incomingDEdge.getSrc().getFeature("train_l");
                feature.element = null;
                Vertex tmpSrc = incomingDEdge.getSrc().copy();
                tmpSrc.setFeature("train_l", feature);
                ctx.output(TRAIN_TEST_SPLIT_OUTPUT, new GraphOp(Op.COMMIT, tmpSrc.masterPart(), tmpSrc));
            }
            if (incomingDEdge.getDest().containsFeature("f")) {
                Feature<?, ?> feature = incomingDEdge.getDest().getFeature("f");
                feature.element = null;
                Vertex tmpDest = incomingDEdge.getDest().copy();
                tmpDest.setFeature("f", feature);
                out.collect(new GraphOp(Op.COMMIT, tmpDest.masterPart(), tmpDest));
            }
            if (incomingDEdge.getDest().containsFeature("train_l")) {
                Feature<?, ?> feature = incomingDEdge.getDest().getFeature("train_l");
                feature.element = null;
                Vertex tmpDest = incomingDEdge.getDest().copy();
                tmpDest.setFeature("train_l", feature);
                ctx.output(TRAIN_TEST_SPLIT_OUTPUT, new GraphOp(Op.COMMIT, tmpDest.masterPart(), tmpDest));
            }

        }
    }

    public static class Parser extends ProcessFunction<String, GraphOp> {
        final String labelFileName;

        final String featureFileName;

        public transient NDArray vertexFeatures;

        public transient NDArray vertexLabels;

        public Set<Integer> seenVertices;

        private int count = 0;

        public Parser(String labelFileName, String featureFileName) throws Exception {
            this.labelFileName = labelFileName;
            this.featureFileName = featureFileName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            FileInputStream vertexFeaturesIn = new FileInputStream(featureFileName);
            FileInputStream vertexLabelsIn = new FileInputStream(labelFileName);
            vertexFeatures = NDHelper.decodeNumpy(LifeCycleNDManager.getInstance(), vertexFeaturesIn);
            vertexLabels = NDHelper.decodeNumpy(LifeCycleNDManager.getInstance(), vertexLabelsIn);
            vertexFeatures.delay();
            vertexLabels.delay();
            seenVertices = new HashSet<>(10000);
        }

        @Override
        public void processElement(String value, ProcessFunction<String, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            if (count == 20000000) System.out.println(seenVertices.size() + "FINISHEDDDD");
            if (count++ > 20000000) return;
            String[] vIds = value.split("\t");
            Vertex src = new Vertex(vIds[0]);
            Vertex dest = new Vertex(vIds[1]);
            int[] vIdInt = new int[]{Integer.parseInt(vIds[0]), Integer.parseInt(vIds[1])};
            if (!seenVertices.contains(vIdInt[0])) {
                src.setFeature("f", new Tensor(vertexFeatures.get(vIdInt[0])));
                src.setFeature("train_l", new Tensor(vertexLabels.get(vIdInt[0]), true, (short) -1));
                seenVertices.add(vIdInt[0]);
            }
            if (!seenVertices.contains(vIdInt[1])) {
                dest.setFeature("f", new Tensor(vertexFeatures.get(vIdInt[1])));
                dest.setFeature("train_l", new Tensor(vertexLabels.get(vIdInt[1]), true, (short) -1));
                seenVertices.add(vIdInt[1]);
            }
            out.collect(new GraphOp(Op.COMMIT, new DEdge(src, dest)));
        }
    }
}
