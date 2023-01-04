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
import it.unimi.dsi.fastutil.PriorityQueues;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
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
        String nodeLabelsFileName = Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_labels.csv").toString();
        SingleOutputStreamOperator<GraphOp> edgeList = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(edgeListFileName)), edgeListFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name("DGraphFin Edges").setParallelism(1).map(new ParseEdges()).setParallelism(1);
        SingleOutputStreamOperator<GraphOp> nodeLabels = env.readFile(new TextInputFormat(new org.apache.flink.core.fs.Path(nodeLabelsFileName)), nodeLabelsFileName, processOnce ? FileProcessingMode.PROCESS_ONCE : FileProcessingMode.PROCESS_CONTINUOUSLY, processOnce ? 0 : 1000).name("DGraphFin Labels").setParallelism(1).map(new ParseNodeLabels()).setParallelism(1);
        return edgeList.union(nodeLabels).process(new Joiner()).name("DGraphFin Joiner").setParallelism(1);
    }

    @Override
    public KeyedProcessFunction<PartNumber, GraphOp, GraphOp> getSplitter() {
        return new Splitter();
    }

    protected static class Splitter extends KeyedProcessFunction<PartNumber, GraphOp, GraphOp>{
        @Override
        public void processElement(GraphOp value, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {

            if(value.element.getType() == ElementType.EDGE){
                out.collect(value);
                ctx.output(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT, value);
            }
            else if(value.element.getType() == ElementType.ATTACHED_FEATURE){
                Feature<?,?> feature = (Feature<?, ?>) value.element;
                if(feature.getName().equals("f")){
                    // Feature
                    out.collect(value);
                }else if(feature.getName().equals("l")){
                    // Label
                    ctx.output(OutputTags.TRAIN_TEST_SPLIT_OUTPUT, value);
                }
            }
        }
    }

    protected static class Joiner extends ProcessFunction<GraphOp, GraphOp> {

        transient ObjectOpenHashSet<String> seenVertices;

        transient Object2ObjectOpenHashMap<String, GraphOp> pendingLabels;

        transient PriorityQueue<Tuple2<GraphOp, Long>> labelsDelayed;

        transient NDArray vertexFeatures;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            vertexFeatures = NDHelper.decodeNumpy(BaseNDManager.getManager(), new FileInputStream(
                    Path.of(System.getenv("DATASET_DIR"), "DGraphFin", "node_features.npy").toString()
            ));
            vertexFeatures.delay();
            seenVertices = new ObjectOpenHashSet<>();
            pendingLabels = new Object2ObjectOpenHashMap<>();
            labelsDelayed = new ObjectHeapPriorityQueue<>(new Comparator<Tuple2<GraphOp, Long>>() {
                @Override
                public int compare(Tuple2<GraphOp, Long> o1, Tuple2<GraphOp, Long> o2) {
                    return o1.f1.compareTo(o2.f1);
                }
            });
        }

        public void checkTimers(long timestamp, Collector<GraphOp> out){
            try{
                if(labelsDelayed.isEmpty()) return;
                Tuple2<GraphOp, Long> val;
                while((val = labelsDelayed.first()).f1 <= timestamp){
                    labelsDelayed.dequeue();
                    out.collect(val.f0);
                    val.f0.resume();
                }
            }catch (NoSuchElementException ignored){}
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            checkTimers(ctx.timerService().currentProcessingTime(), out);
            if(value.element.getType() == ElementType.EDGE){
                out.collect(value);
                DirectedEdge edge = (DirectedEdge) value.element;
                if(!seenVertices.contains(edge.getSrcId())){
                    int vertexIndex = Integer.parseInt(edge.getSrcId());
                    Tensor feature = new Tensor("f", vertexFeatures.get(vertexIndex));
                    feature.id.f0 = ElementType.VERTEX;
                    feature.id.f1 = edge.getSrcId();
                    out.collect(new GraphOp(Op.ADD, feature));
                    seenVertices.add(edge.getSrcId());
                }
                if(!seenVertices.contains(edge.getDestId())){
                    int vertexIndex = Integer.parseInt(edge.getDestId());
                    Tensor feature = new Tensor("f", vertexFeatures.get(vertexIndex));
                    feature.id.f0 = ElementType.VERTEX;
                    feature.id.f1 = edge.getDestId();
                    out.collect(new GraphOp(Op.ADD, feature));
                    seenVertices.add(edge.getDestId());
                }
                if(pendingLabels.containsKey(edge.getSrcId())){
                    GraphOp srcLabel = pendingLabels.remove(edge.getSrcId());
                    labelsDelayed.enqueue(Tuple2.of(srcLabel, ctx.timerService().currentProcessingTime() + ThreadLocalRandom.current().nextInt(100,2000)));
                }
                if(pendingLabels.containsKey(edge.getDestId())){
                    GraphOp destLabel = pendingLabels.remove(edge.getDestId());
                    labelsDelayed.enqueue(Tuple2.of(destLabel, ctx.timerService().currentProcessingTime() + ThreadLocalRandom.current().nextInt(100,2000)));
                }
            }else if(value.element.getType() == ElementType.ATTACHED_FEATURE){
                Tensor tensor = (Tensor) value.element;
                value.delay();
                if(seenVertices.contains(tensor.getAttachedElementId())) labelsDelayed.enqueue(Tuple2.of(value, ctx.timerService().currentProcessingTime() + ThreadLocalRandom.current().nextInt(100,2000)));
                else {
                    pendingLabels.put((String) tensor.getAttachedElementId(), value);
                }
            }

        }
    }

    protected static class ParseEdges implements MapFunction<String, GraphOp>{
        @Override
        public GraphOp map(String value) throws Exception {
            String[] srcDestTs = value.split(",");
            return new GraphOp(Op.ADD, new DirectedEdge(new Vertex(srcDestTs[0]), new Vertex(srcDestTs[1]), srcDestTs[2]));
        }
    }

    protected static class ParseNodeLabels implements MapFunction<String, GraphOp>{

        @Override
        public GraphOp map(String value) throws Exception {
            String[] vertexIdLabel = value.split(",");
            Tensor label = new Tensor("l", BaseNDManager.getManager().create(Short.parseShort(vertexIdLabel[1])),true);
            label.id.f0 = ElementType.VERTEX;
            label.id.f1 = vertexIdLabel[0];
            return new GraphOp(Op.ADD, label);
        }
    }

}
