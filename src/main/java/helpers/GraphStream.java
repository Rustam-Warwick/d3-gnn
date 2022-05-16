package helpers;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import ai.djl.serializers.NDArraySerializer;
import ai.djl.serializers.NDManagerSerializer;
import ai.djl.serializers.ParameterSerializer;
import com.esotericsoftware.kryo.Kryo;
import elements.*;
import features.Set;
import features.VTensor;
import functions.gnn_layers.StreamingGNNLayerFunction;
import functions.selectors.PartKeySelector;
import iterations.Rmi;
import operators.BaseWrapperOperator;
import operators.SimpleTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.objenesis.strategy.StdInstantiatorStrategy;
import partitioner.BasePartitioner;
import storage.BaseStorage;

import java.util.List;

public class GraphStream {
    public short parallelism; // Default parallelism
    public short layers = 0; // Number of GNN Layers in the pipeline
    public IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending
    public short position_index = 1; // Counter of the Current GNN layer being deployed
    public double lambda = 1; // GNN operator explosion coefficient. 1 means no explosion
    public StreamExecutionEnvironment env; // Stream environment

    public GraphStream(StreamExecutionEnvironment env, short layers) {
        this.env = env;
        this.parallelism = (short) this.env.getParallelism();
        this.layers = layers;
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.getConfig().setAutoWatermarkInterval(3000);
//        this.env.getConfig().enableObjectReuse(); // Optimization
        configureSerializers(this.env);
    }

    private static void configureSerializers(StreamExecutionEnvironment env) {
        env.registerTypeWithKryoSerializer(PtNDArray.class, NDArraySerializer.class);
        env.registerTypeWithKryoSerializer(PtNDManager.class, NDManagerSerializer.class);
        env.registerTypeWithKryoSerializer(Parameter.class, ParameterSerializer.class);
        env.registerType(GraphElement.class);
        env.registerType(ReplicableGraphElement.class);
        env.registerType(Vertex.class);
        env.registerType(Edge.class);
        env.registerType(Feature.class);
        env.registerType(Set.class);
        env.registerType(VTensor.class);
        env.registerType(BaseAggregator.class);
        env.registerType(Rmi.class);
        env.registerType(MeanAggregator.class);
    }

    public static void configureSerializers(Kryo kryo) {
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(PtNDArray.class, new NDArraySerializer());
        kryo.register(PtNDManager.class, new NDManagerSerializer());
        kryo.register(Parameter.class, new ParameterSerializer());
        kryo.register(GraphElement.class);
        kryo.register(ReplicableGraphElement.class);
        kryo.register(Vertex.class);
        kryo.register(Edge.class);
        kryo.register(Feature.class);
        kryo.register(Set.class);
        kryo.register(VTensor.class);
        kryo.register(BaseAggregator.class);
        kryo.register(Rmi.class);
        kryo.register(MeanAggregator.class);
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
    public SingleOutputStreamOperator<GraphOp> gnnLayerNewIteration(DataStream<GraphOp> topologyUpdates, BaseStorage storage) {
        StreamingGNNLayerFunction storageProcess = new StreamingGNNLayerFunction(storage);
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers - 1)));
        IterationID localIterationId = new IterationID();

        KeyedStream<GraphOp, String> keyedLast = topologyUpdates
                .keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> forward = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(storageProcess), localIterationId, position_index, layers)).setParallelism(thisParallelism);
        DataStream<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform("IterationTail", TypeInformation.of(Void.class), new SimpleTailOperator(localIterationId)).setParallelism(thisParallelism);

        forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index > 1) {
            int previousParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 2, this.layers)));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            DataStream<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new SimpleTailOperator(this.lastIterationID)).setParallelism(previousParallelism);
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;
    }


    /**
     * Start of the GNN Chain
     *
     * @param allUpdates External System updates
     * @param storages   List of Storages with corresponding plugins
     * @return Last layer corresponding to vertex embeddings
     */
    public SingleOutputStreamOperator<GraphOp> gnnEmbeddings(DataStream<GraphOp> allUpdates, List<BaseStorage> storages) {
        DataStream<GraphOp> rawTopologicalUpdates = allUpdates.forward().map(item -> {
            item.element.clearFeatures();
            return item;
        }).setParallelism(allUpdates.getParallelism()); // Chain + Clean all the non-topological features
        assert layers > 0;
        SingleOutputStreamOperator<GraphOp> lastLayerInputs = null;
        for (BaseStorage storage : storages) {
            if (lastLayerInputs == null) {
                lastLayerInputs = gnnLayerNewIteration(allUpdates, storage);
            } else {
                lastLayerInputs = gnnLayerNewIteration(rawTopologicalUpdates.union(lastLayerInputs), storage);
            }
        }
        return lastLayerInputs;
    }

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
