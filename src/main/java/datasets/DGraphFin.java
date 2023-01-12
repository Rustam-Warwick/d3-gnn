package datasets;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import elements.DirectedEdge;
import elements.Feature;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.features.Tensor;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

public class DGraphFin extends Dataset {

    @Override
    public boolean isResponsibleFor(String datasetName) {
        return datasetName.equals("DGraphFin");
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String edgeListFileName = Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "edge-list.csv").toString();
        SingleOutputStreamOperator<GraphOp> edgeList = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(edgeListFileName)), edgeListFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name("DGraphFin Edges").setParallelism(1).map(new ParseEdges()).setParallelism(1);
        return edgeList.process(new Joiner()).name("DGraphFin Joiner").setParallelism(1);
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new Splitter();
    }

    protected static class Splitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        protected final float trainProb = 0.2f;

        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            if (value.element.getType() == ElementType.EDGE) {
                out.collect(value);
                ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
            } else if (value.element.getType() == ElementType.ATTACHED_FEATURE) {
                Feature<?, ?> feature = (Feature<?, ?>) value.element;
                if (feature.getName().equals("f")) {
                    // Feature
                    out.collect(value);
                } else if (feature.getName().equals("l")) {
                    // Label
                    if (ThreadLocalRandom.current().nextFloat() < trainProb) feature.id.f2 = "tl"; // Train label
                    ctx.output(OutputTags.TRAIN_TEST_SPLIT_OUTPUT, value);
                }
            }
        }
    }

    /**
     * Tracks vertices and generated features and labels when they are first met
     */
    protected static class Joiner extends ProcessFunction<GraphOp, GraphOp> {

        transient ObjectOpenHashSet<String> seenVertices;

        transient PriorityQueue<Tuple2<GraphOp, Long>> labelsTimer;

        transient NDArray vertexFeatures;

        transient NDArray vertexLabels;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            vertexFeatures = NDHelper.decodeNumpy(BaseNDManager.getManager(), new FileInputStream(
                    Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_features.npy").toString()
            ));
            vertexLabels = NDHelper.decodeNumpy(BaseNDManager.getManager(), new FileInputStream(
                    Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_labels.npy").toString()
            ));
            vertexFeatures.detach();
            vertexLabels.detach();
            seenVertices = new ObjectOpenHashSet<>();
            labelsTimer = new ObjectHeapPriorityQueue<>(new Comparator<Tuple2<GraphOp, Long>>() {
                @Override
                public int compare(Tuple2<GraphOp, Long> o1, Tuple2<GraphOp, Long> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
        }

        public void checkTimers(long timestamp, Collector<GraphOp> out) {
            try {
                if (labelsTimer.isEmpty()) return;
                Tuple2<GraphOp, Long> val;
                while ((val = labelsTimer.first()).f1 <= timestamp) {
                    labelsTimer.dequeue();
                    out.collect(val.f0);
                    val.f0.resume();
                }
            } catch (NoSuchElementException ignored) {
            }
        }

        public void generateFeature(int vertexId, Collector<GraphOp> out) {
            Tensor feature = new Tensor("f", vertexFeatures.get(vertexId));
            feature.id.f0 = ElementType.VERTEX;
            feature.id.f1 = String.valueOf(vertexId);
            out.collect(new GraphOp(Op.ADD, feature));
        }

        public void addLabelToQueue(int vertexId, long processTimestamp) {
            if (vertexLabels.get(vertexId).eq(0).getBoolean() || vertexLabels.get(vertexId).eq(1).getBoolean()) {
                // We only care about the first 2 classes [0->Non-Fraud, 1->Fraud]
                Tensor label = new Tensor("l", vertexLabels.get(vertexId));
                label.id.f0 = ElementType.VERTEX;
                label.id.f1 = String.valueOf(vertexId);
                label.delay();
                labelsTimer.enqueue(Tuple2.of(new GraphOp(Op.ADD, label), processTimestamp + ThreadLocalRandom.current().nextInt(2000, 5000)));
            }
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            checkTimers(ctx.timerService().currentProcessingTime(), out);
            out.collect(value);
            DirectedEdge edge = (DirectedEdge) value.element;
            if (!seenVertices.contains(edge.getSrcId())) {
                int vertexIndex = Integer.parseInt(edge.getSrcId());
                generateFeature(vertexIndex, out);
                addLabelToQueue(vertexIndex, ctx.timerService().currentProcessingTime());
                seenVertices.add(edge.getSrcId());
            }
            if (!seenVertices.contains(edge.getDestId())) {
                int vertexIndex = Integer.parseInt(edge.getDestId());
                generateFeature(vertexIndex, out);
                addLabelToQueue(vertexIndex, ctx.timerService().currentProcessingTime());
                seenVertices.add(edge.getDestId());
            }
        }
    }

    /**
     * Class for parsing the Edges in this dataset
     */
    protected static class ParseEdges implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] srcDestTs = value.split(",");
            return new GraphOp(Op.ADD, new DirectedEdge(new Vertex(srcDestTs[0]), new Vertex(srcDestTs[1]), srcDestTs[2]));
        }
    }

}
