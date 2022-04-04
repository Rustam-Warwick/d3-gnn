package helpers;

import aggregators.BaseAggregator;
import aggregators.SumAggregator;
import ai.djl.pytorch.engine.PtNDArray;
import elements.*;
import features.Set;
import features.VTensor;
import functions.GraphLossFn;
import functions.StreamingGNNLayerFunction;
import iterations.BackwardFilter;
import iterations.ForwardFilter;
import iterations.IterateFilter;
import operators.GNNKeyedProcessOperator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import partitioner.BasePartitioner;
import scala.Tuple2;
import serializers.TensorSerializer;

import java.io.File;
import java.util.Objects;

public class GraphStream {
    public short parallelism;
    public short layers;
    public short position_index = 1;
    public short layer_parallelism = 2;
    public StreamExecutionEnvironment env;
    private IterativeStream<GraphOp> iterator = null;

    public GraphStream(short parallelism, short layers) {
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(this.parallelism);
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.getConfig().setAutoWatermarkInterval(30000);
        this.env.setMaxParallelism(32);
        configureSerializers(this.env);
    }

    public static void configureSerializers(StreamExecutionEnvironment env) {
        env.registerTypeWithKryoSerializer(JavaTensor.class, TensorSerializer.class);
        env.registerTypeWithKryoSerializer(PtNDArray.class, TensorSerializer.class);
        env.registerType(GraphElement.class);
        env.registerType(ReplicableGraphElement.class);
        env.registerType(Vertex.class);
        env.registerType(Edge.class);
        env.registerType(Feature.class);
        env.registerType(Set.class);
        env.registerType(JavaTensor.class);
        env.registerType(VTensor.class);
        env.registerType(BaseAggregator.class);
//        env.registerType(MeanAggregator.class);
        env.registerType(SumAggregator.class);
    }

    /**
     * Read the socket stream and parse it
     *
     * @param parser MapFunction to parse the incoming Strings to GraphElements
     * @param host   Host name
     * @param port   Port number
     * @return Datastream of GraphOps
     */
    public DataStream<GraphOp> readSocket(MapFunction<String, GraphOp> parser, String host, int port) {
        return this.env.socketTextStream(host, port).map(parser).name("Input Stream Parser");
    }

    public DataStream<GraphOp> readSocket(FlatMapFunction<String, GraphOp> parser, String host, int port) {
        return this.env.socketTextStream(host, port).flatMap(parser).name("Input Stream Parser");
    }

    /**
     * Read the file and parse it
     *
     * @param parser   MapFunction to parse the incoming Stringsto GraphElements
     * @param fileName Name of the file in local or distributed file system
     * @return Datastream of GraphOps
     */
    public DataStream<GraphOp> readTextFile(MapFunction<String, GraphOp> parser, String fileName) {
        return this.env.readFile(new TextInputFormat(Path.fromLocalFile(new File(fileName))), fileName, FileProcessingMode.PROCESS_CONTINUOUSLY, 120000).setParallelism(1).map(parser).setParallelism(1).name("Input Stream Parser");
    }

    /**
     * Partition the incoming GraphOp Stream into getMaxParallelism() number of subtasks
     *
     * @param stream      Incoming GraphOp Stream
     * @param partitioner Partitioner MapFunction class
     * @return Partitioned and keyed DataStream of GraphOps.
     */
    public DataStream<GraphOp> partition(DataStream<GraphOp> stream, BasePartitioner partitioner) {
        partitioner.partitions = (short) this.env.getMaxParallelism();
        short part_parallelism = this.parallelism;
        if (!partitioner.isParallel()) part_parallelism = 1;
        return stream.map(partitioner).setParallelism(part_parallelism).name("Partitioner");
    }

    /**
     * Given DataStream of GraphOps acts as storage layer with plugins to handle GNN-layers. Stack then for deeper GNNs
     *
     * @param last           DataStream of GraphOp Records, This one shuold be forwardable so it should be already keyedby and with correct parallelism
     * @param storageProcess Process with attached plugins
     * @return DataStream of GraphOps to be stacked
     */
    public DataStream<GraphOp> gnnLayer(DataStream<GraphOp> last, StreamingGNNLayerFunction storageProcess) {
        storageProcess.layers = this.layers;
        storageProcess.position = this.position_index;
        IterativeStream<GraphOp> localIterator = last.iterate();
        KeyedStream<GraphOp, String> ks = DataStreamUtils.reinterpretAsKeyedStream(localIterator, new PartKeySelector());

        KeyedStream<GraphOp, String> res = ks.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new GNNKeyedProcessOperator(storageProcess)).setParallelism(localIterator.getParallelism()).keyBy(new PartKeySelector());
        DataStream<GraphOp> iterateFilter = res.filter(new IterateFilter()).setParallelism(localIterator.getParallelism());
        DataStream<GraphOp> forwardFilter = res.filter(new ForwardFilter()).setParallelism((int) Math.pow(this.layer_parallelism, Math.min(this.position_index + 1, this.layers)));
        if (Objects.nonNull(this.iterator)) {
            DataStream<GraphOp> backFilter = res.filter(new BackwardFilter()).returns(GraphOp.class).setParallelism(this.iterator.getParallelism());
            this.iterator.closeWith(backFilter);
        }
        localIterator.closeWith(iterateFilter);
        this.iterator = localIterator;
        this.position_index++;
        return forwardFilter;
    }

    /**
     * With some p probability split the stream into 2. First one is the normal stream and the second one is the training stream
     *
     * @param inputStream S
     * @return Output GraphStream with training features
     */
    public Tuple2<DataStream<GraphOp>, DataStream<GraphOp>> trainTestSplit(DataStream<GraphOp> inputStream, MapFunction<GraphOp, Tuple2<GraphOp, Boolean>> splitter) {
        DataStream<Tuple2<GraphOp, Boolean>> splittedStream = inputStream.map(splitter);
        DataStream<GraphOp> normalStream = splittedStream.filter(item -> !item._2).map(item -> item._1);
        DataStream<GraphOp> trainingStream = splittedStream.filter(item -> item._2).map(item -> item._1);
        return new Tuple2<>(normalStream, trainingStream);
    }

    public DataStream<GraphOp> gnnLoss(DataStream<GraphOp> predictionStream, DataStream<GraphOp> labelStream) {
        DataStream<GraphOp> lossGrad = predictionStream.join(labelStream)
                .where(new ElementIdSelector()).equalTo(new ElementIdSelector())
                .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(5)))
                .evictor(new KeepLastElement())
                .apply(new GraphLossFn())
                .keyBy(new PartKeySelector()).map(item -> item).setParallelism(this.iterator.getParallelism());

        this.iterator.closeWith(lossGrad);
        return lossGrad;
    }

}
