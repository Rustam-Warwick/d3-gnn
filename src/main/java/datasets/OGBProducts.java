package datasets;

import ai.djl.ndarray.BaseNDManager;
import elements.DirectedEdge;
import elements.Feature;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.features.Tensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Collector;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * OGBProducts Dataset object
 */
public class OGBProducts extends Dataset {

    @Override
    public DataStream<GraphOp> build(StreamExecutionEnvironment env) {
        String topologyOperatorName = "OGB-Products";
        String topologyFileName = Path.of(System.getenv("DATASET_DIR"), "ogb-products", "edges.csv").toString();
        String featureFileName = Path.of(System.getenv("DATASET_DIR"), "ogb-products", "node_features.csv").toString();
        String labelFileName = Path.of(System.getenv("DATASET_DIR"), "ogb-products", "node_labels.csv").toString();
        SingleOutputStreamOperator<GraphOp> topologyStream = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path()), topologyFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000)
                .name(topologyOperatorName).setParallelism(1).map(new TopologyParser()).name(String.format("Parser %s", topologyOperatorName)).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> featureStream = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path()), featureFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000)
                .name(topologyOperatorName).setParallelism(1).map(new FeatureParser()).name(String.format("FeatureParser %s", topologyOperatorName)).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> labelStream = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path()), labelFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000)
                .name(topologyOperatorName).setParallelism(1).map(new LabelParser()).name(String.format("LabelParser %s", topologyOperatorName)).setParallelism(1);
        return topologyStream.union(featureStream, labelStream).flatMap(new Joiner()).setParallelism(1).name(String.format("Joiner %s", topologyOperatorName));
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new Splitter();
    }

    @Override
    public boolean isResponsibleFor(String datasetName) {
        return datasetName.equals("ogb-products");
    }

    protected static class Splitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp> {
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            switch (value.element.getType()) {
                case EDGE: {
                    out.collect(value);
                    ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
                    break;
                }
                case ATTACHED_FEATURE: {
                    Feature<?, ?> feature = (Feature<?, ?>) value.element;
                    switch (feature.getName()) {
                        case "f": {
                            out.collect(value);
                            break;
                        }
                        case "l": {
                            ctx.output(OutputTags.TRAIN_TEST_SPLIT_OUTPUT, value);
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Joining all the label, topology and features in a single stream to have consistent updates
     */
    protected static class Joiner extends RichFlatMapFunction<GraphOp, GraphOp> {

        transient Set<String> seenVertices;

        transient Map<String, GraphOp> pendingTensors;

        transient Map<String, GraphOp> pendingLabels;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            seenVertices = new HashSet<>();
            pendingLabels = new HashMap<>();
            pendingTensors = new HashMap<>();
        }

        @Override
        public void flatMap(GraphOp value, Collector<GraphOp> out) throws Exception {
            switch (value.element.getType()) {
                case EDGE: {
                    DirectedEdge edge = (DirectedEdge) value.element;
                    out.collect(value);
                    if (!seenVertices.contains(edge.getSrcId())) {
                        if (pendingTensors.containsKey(edge.getSrcId())) {
                            GraphOp tmp = pendingTensors.remove(edge.getSrcId());
                            tmp.resume();
                            out.collect(tmp);
                        }
                        if (pendingLabels.containsKey(edge.getSrcId())) {
                            GraphOp tmp = pendingLabels.remove(edge.getSrcId());
                            out.collect(tmp);
                        }
                        seenVertices.add(edge.getSrcId());
                    }

                    if (!seenVertices.contains(edge.getDestId())) {
                        if (pendingTensors.containsKey(edge.getDestId())) {
                            GraphOp tmp = pendingTensors.remove(edge.getDestId());
                            tmp.resume();
                            out.collect(tmp);
                        }
                        if (pendingLabels.containsKey(edge.getDestId())) {
                            GraphOp tmp = pendingLabels.remove(edge.getDestId());
                            out.collect(tmp);
                        }
                        seenVertices.add(edge.getDestId());
                    }

                    break;
                }
                case ATTACHED_FEATURE: {
                    Feature<?, ?> feature = (Feature<?, ?>) value.element;
                    if (seenVertices.contains(feature.id.f1)) {
                        out.collect(value);
                        return;
                    }
                    if (feature.getName().equals("f")) {
                        pendingTensors.put(feature.id.f1, value);
                        value.delay();
                    }
                    if (feature.getName().equals("l")) pendingLabels.put(feature.id.f1, value);
                }
            }

        }
    }

    /**
     * Parsing the label file from the CSV file
     */
    protected static class LabelParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            Feature<Integer, Integer> label = new Feature<>("l", Integer.parseInt(values[1]), true);
            label.id.f0 = ElementType.VERTEX;
            label.id.f1 = values[0];
            return new GraphOp(Op.COMMIT, label);
        }
    }

    /**
     * Parsing the Feature data from the CSV file
     */
    protected static class FeatureParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] values = value.split(",");
            float[] featureFloat = new float[values.length - 1];
            for (int i = 1; i < values.length; i++) {
                featureFloat[i - 1] = Float.parseFloat(values[i]);
            }
            Tensor feature = new Tensor("f", BaseNDManager.getManager().create(featureFloat));
            feature.id.f0 = ElementType.VERTEX;
            feature.id.f1 = values[0];
            return new GraphOp(Op.COMMIT, feature);
        }
    }

    /**
     * Parsing the topology data from CSV file
     */
    protected static class TopologyParser implements MapFunction<String, GraphOp> {
        @Override
        public GraphOp map(String value) throws Exception {
            String[] ids = value.split(",");
            return new GraphOp(Op.COMMIT, new DirectedEdge(new Vertex(ids[0]), new Vertex(ids[1])));
        }
    }
}
