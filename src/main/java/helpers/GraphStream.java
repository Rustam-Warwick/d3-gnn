package helpers;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import elements.*;
import features.Set;
import features.VTensor;
import functions.StreamingGNNLayerFunction;
import iterations.Rmi;
import operators.GNNKeyedProcessOperator;
import operators.SimpleTailOperator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import partitioner.BasePartitioner;
import serializers.JavaTensor;
import serializers.TensorSerializer;

public class GraphStream {
    public short parallelism; // Default parallelism
    public short layers; // Number of GNN Layers in the pipeline
    public IterationID lastIterationID; // Previous Layer Iteration Id
    public short position_index = 1; // Counter of the Current GNN layer being deployed
    public double lambda = 1.8; // GNN operator explosion coefficient
    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tableEnv;

    public GraphStream(short parallelism, short layers) {
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
//        this.tableEnv = StreamTableEnvironment.create(this.env);
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.setParallelism(this.parallelism);
        this.env.getConfig().enableObjectReuse();
        this.env.setMaxParallelism(128);
        configureSerializers();
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
        env.registerType(Rmi.class);
        env.registerType(MeanAggregator.class);
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
        return env.readTextFile(fileName).flatMap(parser).name("Input Stream Parser");
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

    //
    public DataStream<GraphOp> gnnLayerOldIteration(DataStream<GraphOp> last, StreamingGNNLayerFunction storageProcess) {
        storageProcess.numLayers = this.layers;
        storageProcess.position = this.position_index;
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers)));
        DataStream<GraphOp> input = last.keyBy(new PartKeySelector()).map(item -> item).setParallelism(thisParallelism);
        IterativeStream<GraphOp> currentIteration = input.iterate();
        KeyedStream<GraphOp, String> ks = currentIteration.keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> forwardFilter = ks.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new GNNKeyedProcessOperator(storageProcess, new IterationID())).setParallelism(thisParallelism);
        DataStream<GraphOp> iterateFilter = forwardFilter.getSideOutput(new OutputTag<>("iterate", TypeInformation.of(GraphOp.class)));
        currentIteration.closeWith(iterateFilter);

        input.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        currentIteration.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        forwardFilter.getTransformation().setCoLocationGroupKey("gnn-" + position_index);


        this.position_index++;
        return forwardFilter;
    }


    public DataStream<GraphOp> gnnLayerNewIteration(DataStream<GraphOp> last, StreamingGNNLayerFunction storageProcess) {
        storageProcess.numLayers = this.layers;
        storageProcess.position = this.position_index;
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers - 1)));
        IterationID localIterationId = new IterationID();

        DataStream<GraphOp> keyedLast = last.keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> forwardFilter = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new GNNKeyedProcessOperator(storageProcess, localIterationId)).setParallelism(thisParallelism);
        DataStream<GraphOp> iterateFilter = forwardFilter.getSideOutput(new OutputTag<>("iterate", TypeInformation.of(GraphOp.class)));
        DataStream<Void> iteration = iterateFilter.keyBy(new PartKeySelector()).transform("IterationTail", TypeInformation.of(Void.class), new SimpleTailOperator(localIterationId)).setParallelism(thisParallelism);

        forwardFilter.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        iteration.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index > 1) {
            int previousParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 2, this.layers)));
            DataStream<GraphOp> backFilter = forwardFilter.getSideOutput(new OutputTag<>("backward", TypeInformation.of(GraphOp.class)));
            DataStream<Void> backiteration = backFilter.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new SimpleTailOperator(this.lastIterationID)).setParallelism(previousParallelism);
            backiteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
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
        DataStream<GraphOp> losses = trainingStream.process(lossFunction).setParallelism(1);
        DataStream<Void> backIteration = losses.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new SimpleTailOperator(this.lastIterationID)).setParallelism(trainingStream.getParallelism());
        backIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        return losses;
    }


}
