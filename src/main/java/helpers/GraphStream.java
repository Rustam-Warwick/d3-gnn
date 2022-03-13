package helpers;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import aggregators.SumAggregator;
import ai.djl.mxnet.engine.MxNDArray;
import elements.*;
import features.Set;
import features.Tensor;
import features.VTensor;
import functions.GraphLossFn;
import functions.GraphProcessFn;
import iterations.ForwardFilter;
import iterations.IterateFilter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import partitioner.BasePartitioner;
import partitioner.PartKeySelector;
import partitioner.FeatureParentKeySelector;
import serializers.TensorSerializer;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

public class GraphStream {
    public short parallelism;
    public short layers;
    public short position_index = 1;
    public short layer_parallelism = 2;
    public StreamExecutionEnvironment env;
    public DataStream<GraphOp> last = null;
    private IterativeStream<GraphOp> iterator = null;

    public static void configureSerializers(StreamExecutionEnvironment env){
        env.registerTypeWithKryoSerializer(MxNDArray.class, TensorSerializer.class);
        env.registerType(GraphElement.class);
        env.registerType(ReplicableGraphElement.class);
        env.registerType(Vertex.class);
        env.registerType(Edge.class);
        env.registerType(Feature.class);
        env.registerType(Set.class);
        env.registerType(Tensor.class);
        env.registerType(VTensor.class);
        env.registerType(BaseAggregator.class);
        env.registerType(MeanAggregator.class);
        env.registerType(SumAggregator.class);
    }
    public GraphStream(short parallelism, short layers) {
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(this.parallelism);
        this.env.setMaxParallelism(8);
        configureSerializers(this.env);
    }

    public DataStream<GraphOp> readSocket(MapFunction<String, GraphOp> parser, String host, int port) {
        this.last = this.env.socketTextStream(host, port).map(parser).name("Input Stream Parser");
        return this.last;
    }

    public DataStream<GraphOp> readTextFile(MapFunction<String, GraphOp> parser, String fileName) {
        return this.env.readFile(new TextInputFormat(Path.fromLocalFile(new File(fileName))), fileName, FileProcessingMode.PROCESS_CONTINUOUSLY, 120000).setParallelism(1).map(parser).setParallelism(1).name("Input Stream Parser");
    }

    public DataStream<GraphOp> partition(DataStream<GraphOp> stream, BasePartitioner partitioner) {
        partitioner.partitions = (short) this.env.getMaxParallelism();
        short part_parallelism = this.parallelism;
        if (!partitioner.isParallel()) part_parallelism = 1;
        this.last = this.last.map(partitioner).setParallelism(part_parallelism).name("Partitioner").keyBy(new PartKeySelector());
        this.last = this.last.map(item -> item).setParallelism(this.layer_parallelism);
        return this.last;
    }

    public KeyedStream<GraphOp, Short> keyBy(DataStream<GraphOp> last) {
        Constructor<KeyedStream> constructor = null;
        try {
            constructor = KeyedStream.class.getDeclaredConstructor(DataStream.class, PartitionTransformation.class, KeySelector.class, TypeInformation.class);
            constructor.setAccessible(true);
            KeySelector<GraphOp, Short> keySelector = new KeySelector<GraphOp, Short>() {
                @Override
                public Short getKey(GraphOp value) throws Exception {
                    return value.part_id;
                }
            };
            return constructor.newInstance(
                    last,
                    new PartitionTransformation<>(
                            last.getTransformation(),
                            new MyKeyGroupPartitioner(keySelector, StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
                    keySelector,
                    TypeInformation.of(Short.class)
            );
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
    }

    public DataStream<GraphOp> gnnLayer(GraphProcessFn storageProcess) {
        storageProcess.layers = this.layers;
        storageProcess.position = this.position_index;
        IterativeStream<GraphOp> localIterator = this.last.iterate();
        KeyedStream<GraphOp, String> ks = DataStreamUtils.reinterpretAsKeyedStream(localIterator, new PartKeySelector());
        KeyedStream<GraphOp, String> res = ks.process(storageProcess).name("Gnn Process").setParallelism(localIterator.getParallelism()).keyBy(new PartKeySelector());
        DataStream<GraphOp> iterateFilter = res.filter(new IterateFilter()).setParallelism(localIterator.getParallelism());
        DataStream<GraphOp> forwardFilter = res.filter(new ForwardFilter()).setParallelism((int) Math.pow(this.layer_parallelism, Math.min(this.position_index + 1, this.layers)));
        if (Objects.nonNull(this.iterator)) {
//            DataStream<GraphOp> backFilter = res.filter(new BackwardFilter()).returns(GraphOp.class).setParallelism(this.iterator.getParallelism());
//            this.iterator.closeWith(backFilter);
        }
        localIterator.closeWith(iterateFilter);
        this.last = forwardFilter;
        this.iterator = localIterator;
        this.position_index++;
        return this.last;
    }

    public DataStream<GraphOp> gnnLoss(DataStream<GraphOp>labelStream) {
        this.last.coGroup(labelStream)
                .where(new FeatureParentKeySelector()).equalTo(new FeatureParentKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .apply(new GraphLossFn());
    }

}
