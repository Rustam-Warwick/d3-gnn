package datasets;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import ai.djl.ndarray.types.DataType;
import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.Op;
import elements.features.Tensor;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.interfaces.GraphRuntimeContext;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

public class DGraphFin extends Dataset {

    @CommandLine.Option(names = {"--dGraphFin:trainSplitProb"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = {"Probability of train labels [0, 1]"})
    protected float trainSplitProb;

    @CommandLine.Option(names = {"--dGraphFin:deltaBoundMs"}, defaultValue = "0", fallbackValue = "0", arity = "1", description = {"Bound of milliseconds to wait before emitting labels. Coalesed by 100ms intervals"})
    protected int deltaBoundMs;

    @CommandLine.Option(names = {"--dGraphFin:withLabels"}, defaultValue = "true", fallbackValue = "true", arity = "1", description = {"Stream the labels as well"})
    protected boolean withLabels;

    @Override
    public boolean isResponsibleFor(String datasetName) {
        return datasetName.equals("DGraphFin");
    }

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String edgeListFileName = Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "edge-list.csv").toString();
        return env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(edgeListFileName)), edgeListFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000)
                .name("DGraphFin Edges")
                .setParallelism(1)
                .flatMap(new ParseEdges())
                .name("DGraphFin Parser")
                .setParallelism(1)
                .process(new Joiner(withLabels))
                .name("DGraphFin Joiner")
                .setParallelism(1);
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new Splitter(trainSplitProb, deltaBoundMs, withLabels);
    }

    protected static class Splitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {

        /**
         * Probability of splitting train and test labels
         */
        protected final float trainSplitProb;

        /**
         * Upper bound on waiting before emitting the labels in ms
         */
        protected final int deltaBoundMs;

        /**
         * Include the labels
         */
        protected final boolean withLabels;

        /**
         * Queue of timers for emitting the labels
         */
        transient PriorityQueue<Tuple2<GraphOp, Long>> labelsTimer;

        /**
         * Runtime context of Splitter Operator
         */
        transient GraphRuntimeContext graphRuntimeContext;

        public Splitter(float trainSplitProb, int deltaBound, boolean withLabels) {
            this.trainSplitProb = trainSplitProb;
            this.deltaBoundMs = deltaBound;
            this.withLabels = withLabels;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            graphRuntimeContext = GraphRuntimeContext.CONTEXT_THREAD_LOCAL.get();
            labelsTimer = new ObjectHeapPriorityQueue<>(new Comparator<Tuple2<GraphOp, Long>>() {
                @Override
                public int compare(Tuple2<GraphOp, Long> o1, Tuple2<GraphOp, Long> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
        }

        /**
         * Add this label to queue for further delayed propagation
         */
        public void addLabelToQueue(Tensor label) {
            if (label.value.gt(1).getBoolean()) return; // Only retrain Fraud and non-fraud labels
            if (ThreadLocalRandom.current().nextFloat() < trainSplitProb)
                label.id.f2 = "tl"; // Mark label as training label
            GraphOp labelOp = new GraphOp(Op.ADD, label.getMasterPart(), label);
            if (deltaBoundMs == 0) {
                graphRuntimeContext.output(labelOp, OutputTags.TRAIN_TEST_SPLIT_OUTPUT);
                return;
            }
            labelOp.delay();
            int delta = (int) ThreadLocalRandom.current().nextDouble(0, deltaBoundMs);
            long updateTime = graphRuntimeContext.getTimerService().currentProcessingTime() + delta;
            long coalescedTime = (long) (Math.ceil((updateTime) / 100.0) * 100);
            labelsTimer.enqueue(Tuple2.of(labelOp, updateTime));
            graphRuntimeContext.getTimerService().registerProcessingTimeTimer(coalescedTime);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Tuple2<GraphOp, Long> val;
            while (!labelsTimer.isEmpty() && (val = labelsTimer.first()).f1 <= timestamp) {
                labelsTimer.dequeue();
                ctx.output(OutputTags.TRAIN_TEST_SPLIT_OUTPUT, val.f0);
                val.f0.resume();
            }
        }

        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            DirectedEdge edge = (DirectedEdge) value.element;
            GraphOp srcFeatureOp = null;
            GraphOp destFeatureOp = null;
            if (edge.src.features != null) {
                Tensor featureSrc = (Tensor) edge.src.getFeature("f");
                srcFeatureOp = new GraphOp(Op.ADD, featureSrc.getMasterPart(), featureSrc);
                if (withLabels) {
                    Tensor labelSrc = (Tensor) edge.src.getFeature("l");
                    addLabelToQueue(labelSrc);
                }
                edge.src.features = null;
            }
            if (edge.dest.features != null) {
                Tensor featureSrc = (Tensor) edge.dest.getFeature("f");
                destFeatureOp = new GraphOp(Op.ADD, featureSrc.getMasterPart(), featureSrc);
                if (withLabels) {
                    Tensor labelSrc = (Tensor) edge.dest.getFeature("l");
                    addLabelToQueue(labelSrc);
                }
                edge.dest.features = null;
            }
            out.collect(value);
            ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
            if (srcFeatureOp != null) out.collect(srcFeatureOp);
            if (destFeatureOp != null) out.collect(destFeatureOp);
        }
    }

    protected static class Joiner extends ProcessFunction<GraphOp, GraphOp> {

        final boolean withLabels;

        transient IntOpenHashSet seenVertices;

        transient NDArray vertexFeatures;

        transient NDArray vertexLabels;

        public Joiner(boolean withLabels) {
            this.withLabels = withLabels;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (withLabels) {
                vertexLabels = NDHelper.decodeNumpy(BaseNDManager.getManager(), new FileInputStream(
                        Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_labels.npy").toString()
                )).toType(DataType.BOOLEAN, false);
                vertexLabels.delay();
            }
            vertexFeatures = NDHelper.decodeNumpy(BaseNDManager.getManager(), new FileInputStream(
                    Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_features.npy").toString()
            ));
            vertexFeatures.delay();
            seenVertices = new IntOpenHashSet();
        }

        @Override
        public void close() throws Exception {
            super.close();
            vertexFeatures.resume();
            if (withLabels) vertexLabels.resume();
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            DirectedEdge edge = (DirectedEdge) value.element;
            int srcVertexIndex = Integer.parseInt(edge.getSrcId());
            int destVertexIndex = Integer.parseInt(edge.getDestId());
            if (!seenVertices.contains(srcVertexIndex)) {
                Tensor feature = new Tensor("f", vertexFeatures.get(srcVertexIndex));
                feature.setElement(edge.getSrc(), false);
                if (withLabels) {
                    Tensor label = new Tensor("l", vertexLabels.get(srcVertexIndex));
                    label.setElement(edge.getSrc(), false);
                }
                seenVertices.add(srcVertexIndex);
            }
            if (!seenVertices.contains(destVertexIndex)) {
                Tensor feature = new Tensor("f", vertexFeatures.get(destVertexIndex));
                feature.setElement(edge.getDest(), false);
                if (withLabels) {
                    Tensor label = new Tensor("l", vertexLabels.get(destVertexIndex));
                    label.setElement(edge.getDest(), false);
                }
                seenVertices.add(destVertexIndex);
            }
            out.collect(value);
        }
    }

    /**
     * Class for parsing the Edges in this dataset
     */
    protected static class ParseEdges implements FlatMapFunction<String, GraphOp> {

        @Override
        public void flatMap(String value, Collector<GraphOp> out) throws Exception {
            String[] srcDestTs = value.split(",");
            out.collect(new GraphOp(Op.ADD, new DirectedEdge(new Vertex(srcDestTs[0]), new Vertex(srcDestTs[1]), srcDestTs[2])));
        }
    }

}
