package helpers;

import aggregators.BaseAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import elements.*;
import features.Set;
import features.VTensor;
import functions.StreamingGNNLayerFunction;
import operators.GNNKeyedProcessOperator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import partitioner.BasePartitioner;
import serializers.TensorSerializer;

import java.io.File;
import java.util.Objects;

public class GraphStream {
    public short parallelism;
    public short layers;
    public short position_index = 1;
    public short layer_parallelism = 3;
    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tableEnv;
    public IterativeStream<GraphOp> iterator = null;

    public GraphStream(short parallelism, short layers) {
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
//        this.tableEnv = StreamTableEnvironment.create(this.env);
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.setParallelism(this.parallelism);
        this.env.getConfig().setAutoWatermarkInterval(30000);
        this.env.setMaxParallelism(36);
        configureSerializers();
    }

    public void createQueriesTable() {
        tableEnv.executeSql("CREATE TABLE blackhole_table (" +
                "  f0 INT," +
                "  f1 INT," +
                "  f2 STRING," +
                "  f3 DOUBLE" +
                ") WITH (" +
                "  'connector' = 'blackhole'" +
                ")");
        tableEnv.listTables();
    }

    public void configureSerializers() {
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
//        env.registerType(SumAggregator.class);
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
    public DataStream<GraphOp> readTextFile(FlatMapFunction<String, GraphOp> parser, String fileName) {
        return this.env.readFile(new TextInputFormat(Path.fromLocalFile(new File(fileName))), fileName, FileProcessingMode.PROCESS_CONTINUOUSLY, 120000).flatMap(parser).name("Input Stream Parser");
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
        storageProcess.numLayers = this.layers;
        storageProcess.position = this.position_index;
        last = last.map(item->item).setParallelism((int) Math.pow(this.layer_parallelism, Math.min(this.position_index, this.layers)));
        IterativeStream<GraphOp> localIterator = last.iterate();
        KeyedStream<GraphOp, String> ks = localIterator.keyBy(new PartKeySelector());

        SingleOutputStreamOperator<GraphOp> forwardFilter = ks.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new GNNKeyedProcessOperator(storageProcess)).setParallelism((int) Math.pow(this.layer_parallelism, Math.min(this.position_index, this.layers)));
        DataStream<GraphOp> iterateFilter = forwardFilter.getSideOutput(new OutputTag<>("iterate", TypeInformation.of(GraphOp.class)));

        if (Objects.nonNull(this.iterator)) {
            DataStream<GraphOp> backFilter = forwardFilter.getSideOutput(new OutputTag<>("backward", TypeInformation.of(GraphOp.class))).map(item -> item).setParallelism(this.iterator.getParallelism());
            this.iterator.closeWith(backFilter);
        }
        localIterator.closeWith(iterateFilter);
        this.iterator = localIterator;
        this.position_index++;
        return forwardFilter;
    }

    public Table toVertexEmbeddingTable(DataStream<GraphOp> output) {
        assert position_index == layers;
        DataStream<Row> embeddings = output.keyBy(new ElementIdSelector()).map(new MapFunction<GraphOp, Row>() {
            @Override
            public Row map(GraphOp value) throws Exception {
                VTensor feature = (VTensor) value.element;
                return Row.of(feature.attachedTo._2, feature.masterPart(), feature.value._1, feature.value._2);
            }
        }).returns(Types.ROW(Types.STRING, TypeInformation.of(Short.class), TypeInformation.of(NDArray.class), Types.INT));
        Table vertexEmbeddingTable = tableEnv.fromDataStream(embeddings, Schema.newBuilder().primaryKey("f0").columnByExpression("event_time", "PROCTIME()").build()).as("id", "master", "feature", "version");
        Table vertexEmbeddingsUpsert = tableEnv.sqlQuery("SELECT id, LAST_VALUE(master), LAST_VALUE(feature), LAST_VALUE(version), LAST_VALUE(event_time) FROM " + vertexEmbeddingTable +
                " GROUP BY id");

        tableEnv.createTemporaryView("vertexEmbeddings", vertexEmbeddingsUpsert);
        return vertexEmbeddingTable;
    }

    public Table toEdgeTable(DataStream<GraphOp> output) {
        assert position_index == layers;
        DataStream<Row> embeddings = output.keyBy(new ElementIdSelector()).map(new MapFunction<GraphOp, Row>() {
            @Override
            public Row map(GraphOp value) throws Exception {
                Edge edge = (Edge) value.element;
                return Row.of(edge.src.getId(), edge.dest.getId());
            }
        }).returns(Types.ROW(Types.STRING, Types.STRING));
        Table vertexEmbeddingTable = tableEnv.fromDataStream(embeddings, Schema.newBuilder().columnByExpression("event_time", "PROCTIME()").primaryKey("f0", "f1").build()).as("srcId", "destId");
        tableEnv.createTemporaryView("edges", vertexEmbeddingTable);
        return vertexEmbeddingTable;
    }

    /**
     * With some p probability split the stream into 2. First one is the normal stream and the second one is the training stream
     *
     * @param inputStream S
     * @return Output GraphStream with training features
     */
    public DataStream<GraphOp> trainTestSplit(DataStream<GraphOp> inputStream, ProcessFunction<GraphOp, GraphOp> splitter) {
        return inputStream.process(splitter);
    }

    public DataStream<GraphOp> gnnLoss(DataStream<GraphOp> trainingStream, ProcessFunction<GraphOp, GraphOp> lossFunction) {
        DataStream<GraphOp> losses = trainingStream.process(lossFunction).setParallelism(1).startNewChain().map(item -> item).setParallelism(iterator.getParallelism());
        this.iterator.closeWith(losses);
        return losses;
    }

}
