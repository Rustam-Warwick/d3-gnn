package helpers;

import aggregators.BaseAggregator;
import aggregators.NewMeanAggregator;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import elements.*;
import features.Set;
import features.VTensor;
import functions.gnn_layers.StreamingGNNLayerFunction;
import functions.nn.JavaTensor;
import functions.selectors.PartKeySelector;
import iterations.Rmi;
import operators.*;
import org.apache.commons.math3.analysis.function.Sin;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.util.OutputTag;
import partitioner.BasePartitioner;
import serializers.ParameterSerializer;
import serializers.TensorSerializer;
import storage.BaseStorage;

import java.util.List;

public class GraphStream {
    public short parallelism; // Default parallelism
    public short layers = 0; // Number of GNN Layers in the pipeline
    public IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending
    public short position_index = 1; // Counter of the Current GNN layer being deployed
    public double lambda = 1.8; // GNN operator explosion coefficient
    public StreamExecutionEnvironment env; // Stream environment

    public GraphStream(StreamExecutionEnvironment env) {
        this.env = env;
        this.parallelism = (short) this.env.getParallelism();
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.getConfig().setAutoWatermarkInterval(3000);
        this.env.getConfig().enableObjectReuse(); // Optimization
        this.env.setMaxParallelism(128);
        configureSerializers();
    }

    private void configureSerializers() {
        env.registerTypeWithKryoSerializer(JavaTensor.class, TensorSerializer.class);
        env.registerTypeWithKryoSerializer(PtNDArray.class, TensorSerializer.class);
        env.registerTypeWithKryoSerializer(Parameter.class, ParameterSerializer.class);
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
        env.registerType(NewMeanAggregator.class);
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
     * Helper function to add a new layer of GNN Iteration, explicitly used to trainer. Otherwise chain starts from @gnnEmbeddings()
     *
     * @param topologyUpdates non-keyed input graphop to this layer, can be a union of several streams as well
     * @param storage         Storage of choice with attached plugins
     * @return output stream dependent on the plugin
     */
    public DataStream<GraphOp> gnnLayerNewIteration(DataStream<GraphOp> topologyUpdates, BaseStorage storage) {
        StreamingGNNLayerFunction storageProcess = new StreamingGNNLayerFunction(storage, position_index, layers);
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers - 1)));
        IterationID localIterationId = new IterationID();

        KeyedStream<GraphOp, String> keyedLast = topologyUpdates
                .keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> iterations = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(storageProcess),localIterationId)).setParallelism(thisParallelism);
//        SingleOutputStreamOperator<GraphOp> iterations = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new GNNKeyedProcessOperator(storageProcess, localIterationId)).setParallelism(thisParallelism);
        DataStream<Void> iterationHandler = iterations.keyBy(new PartKeySelector()).transform("IterationTail", TypeInformation.of(Void.class), new SimpleTailOperator(localIterationId, true)).setParallelism(thisParallelism);

        iterations.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index > 1) {
            int previousParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 2, this.layers)));
            DataStream<GraphOp> backFilter = iterations.getSideOutput(new OutputTag<>("backward", TypeInformation.of(GraphOp.class)));
            DataStream<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new SimpleTailOperator(this.lastIterationID, false)).setParallelism(previousParallelism);
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return iterations
                .getSideOutput(new OutputTag<>("forward", TypeInformation.of(GraphOp.class)))
                .forward()
                .transform("Filter Watermarks", TypeInformation.of(GraphOp.class), new WatermarkTimestampResolverOperator())
                .setParallelism(thisParallelism);
    }


    /**
     * Start of the GNN Chain
     *
     * @param allUpdates External System updates
     * @param storages   List of Storages with corresponding plugins
     * @return Last layer corresponding to vertex embeddings
     */
    public DataStream<GraphOp> gnnEmbeddings(DataStream<GraphOp> allUpdates, List<BaseStorage> storages) {
        this.layers = (short) storages.size();
        DataStream<GraphOp> rawTopologicalUpdates = allUpdates.map(item -> {
            if (item.element != null) item.element.features.clear();
            return item;
        });
        assert layers > 0;
        DataStream<GraphOp> lastLayerInputs = null;
        for (BaseStorage storage : storages) {
            if (lastLayerInputs == null) {
                lastLayerInputs = gnnLayerNewIteration(allUpdates, storage);
            } else {
                lastLayerInputs = gnnLayerNewIteration(rawTopologicalUpdates.union(lastLayerInputs), storage);
            }
        }
        return lastLayerInputs;
    }

//    public Table toVertexEmbeddingTable(DataStream<GraphOp> output) {
//        assert position_index == layers;
//        DataStream<Row> embeddings = output.keyBy(new ElementForPartKeySelector()).map(new MapFunction<GraphOp, Row>() {
//            @Override
//            public Row map(GraphOp value) throws Exception {
//                VTensor feature = (VTensor) value.element;
//                return Row.of(feature.attachedTo._2, feature.masterPart(), feature.value._1, feature.value._2);
//            }
//        }).returns(Types.ROW(Types.STRING, TypeInformation.of(Short.class), TypeInformation.of(NDArray.class), Types.INT));
//        Table vertexEmbeddingTable = tableEnv.fromDataStream(embeddings, Schema.newBuilder().primaryKey("f0").columnByExpression("event_time", "PROCTIME()").build()).as("id", "master", "feature", "version");
//        Table vertexEmbeddingsUpsert = tableEnv.sqlQuery("SELECT id, LAST_VALUE(master), LAST_VALUE(feature), LAST_VALUE(version), LAST_VALUE(event_time) FROM " + vertexEmbeddingTable +
//                " GROUP BY id");
//
//        tableEnv.createTemporaryView("vertexEmbeddings", vertexEmbeddingsUpsert);
//        return vertexEmbeddingTable;
//    }
//
//    public Table toEdgeTable(DataStream<GraphOp> output) {
//        assert position_index == layers;
//        DataStream<Row> embeddings = output.keyBy(new ElementForPartKeySelector()).map(new MapFunction<GraphOp, Row>() {
//            @Override
//            public Row map(GraphOp value) throws Exception {
//                Edge edge = (Edge) value.element;
//                return Row.of(edge.src.getId(), edge.dest.getId());
//            }
//        }).returns(Types.ROW(Types.STRING, Types.STRING));
//        Table vertexEmbeddingTable = tableEnv.fromDataStream(embeddings, Schema.newBuilder().columnByExpression("event_time", "PROCTIME()").primaryKey("f0", "f1").build()).as("srcId", "destId");
//        tableEnv.createTemporaryView("edges", vertexEmbeddingTable);
//        return vertexEmbeddingTable;
//    }

    /**
     * With some p probability split the stream into 2. First one is the normal stream and the second one is the training stream
     *
     * @param inputStream S
     * @return Output GraphStream with training features
     */
    public DataStream<GraphOp> trainTestSplit(DataStream<GraphOp> inputStream, ProcessFunction<GraphOp, GraphOp> splitter) {
        return inputStream.process(splitter).setParallelism(1);
    }

    public DataStream<GraphOp> gnnLoss(DataStream<GraphOp> trainingStream, ProcessFunction<GraphOp, GraphOp> lossFunction) {
        DataStream<GraphOp> losses = trainingStream.process(lossFunction).setParallelism(1);
        DataStream<Void> backIteration = losses.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new SimpleTailOperator(this.lastIterationID)).setParallelism(trainingStream.getParallelism());
        backIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        return losses;
    }


}
