package helpers;
import ai.djl.mxnet.engine.MxNDArray;
import elements.GraphOp;
import functions.GraphProcessFn;
import iterations.IterationState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import partitioner.BasePartitioner;
import serializers.TensorSerializer;

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

    public GraphStream(short parallelism, short layers) {
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(this.parallelism);
        this.env.registerTypeWithKryoSerializer(MxNDArray.class, TensorSerializer.class);
    }

    public DataStream<GraphOp> readSocket(MapFunction<String, GraphOp> parser, String host, int port) {
        this.last = this.env.socketTextStream(host, port).map(parser).name("Input Stream Parser");
        return this.last;
    }
    public DataStream<GraphOp> readTextFile(MapFunction<String, GraphOp> parser, String fileName){
        this.last = this.env.readTextFile(fileName).setParallelism(1).map(parser).setParallelism(1).name("Input Stream Parser");
        return this.last;
    }

    public DataStream<GraphOp> partition(BasePartitioner partitioner) {
        partitioner.partitions = (short)(Math.pow(this.layer_parallelism, this.layers + 1));
        short part_parallelism = this.parallelism;
        if (!partitioner.isParallel()) part_parallelism = 1;
        this.last = this.last.map(partitioner).setParallelism(part_parallelism).name("Partitioner");
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
        IterativeStream<GraphOp> iterator = this.last.iterate();
        KeyedStream<GraphOp, Short> ks = iterator.keyBy(item->item.part_id);
        DataStream<GraphOp> res = ks.process(storageProcess).name("Gnn Process").setParallelism((int) Math.pow(this.layer_parallelism, this.position_index));
        DataStream<GraphOp> iterateFilter = res.filter(item -> item.state == IterationState.ITERATE).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index));
        DataStream<GraphOp> forwardFilter = res.filter(item -> item.state == IterationState.FORWARD).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index + 1));
        if (Objects.nonNull(this.iterator)) {
            DataStream<GraphOp> backFilter = res.filter(item -> item.state == IterationState.BACKWARD).returns(GraphOp.class).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index - 1));
            this.iterator.closeWith(backFilter);
        }
        iterator.closeWith(iterateFilter);
        this.last = forwardFilter;
        this.iterator = iterator;
        this.position_index++;
        return this.last;
    }

    public DataStream<GraphOp> outputLayer(GraphProcessFn storageProcess) {
        storageProcess.layers = this.layers;
        storageProcess.position = this.position_index;
        IterativeStream<GraphOp> iterator = this.last.iterate();
        KeyedStream<GraphOp, Short> ks = iterator.keyBy(item->item.part_id);
        DataStream<GraphOp> res = ks.process(storageProcess).name("Gnn Process").setParallelism((int) Math.pow(this.layer_parallelism, this.position_index));
        DataStream<GraphOp> iterateFilter = res.filter(item -> item.state == IterationState.ITERATE).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index));
        DataStream<GraphOp> forwardFilter = res.filter(item -> item.state == IterationState.FORWARD).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index));
        if (Objects.nonNull(this.iterator)) {
            DataStream<GraphOp> backFilter = res.filter(item -> item.state == IterationState.BACKWARD).returns(GraphOp.class).setParallelism((int) Math.pow(this.layer_parallelism, this.position_index - 1));
            this.iterator.closeWith(backFilter);
        }
        iterator.closeWith(iterateFilter);
        this.last = forwardFilter;
        this.iterator = null; // No more backward after this
        this.position_index++;
        return this.last;
    }
}
