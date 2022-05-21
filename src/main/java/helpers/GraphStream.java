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
import datasets.Dataset;
import elements.*;
import elements.iterations.Rmi;
import features.Set;
import features.VTensor;
import functions.gnn_layers.WindowingGNNLayerFunction;
import functions.selectors.PartKeySelector;
import operators.BaseWrapperOperator;
import operators.IterationTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.objenesis.strategy.StdInstantiatorStrategy;
import partitioner.BasePartitioner;
import storage.BaseStorage;

import java.util.List;
import java.util.function.BiFunction;

public class GraphStream {
    public short parallelism; // Default parallelism
    public short layers;// Number of GNN Layers in the pipeline
    public IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending
    public IterationID fullLoopIterationId; // Iteration Id of 0 layer
    public short position_index; // Counter of the Current GNN layer being
    public double lambda = 1; // GNN operator explosion coefficient. 1 means no explosion
    public final StreamExecutionEnvironment env; // Stream environment

    public GraphStream(StreamExecutionEnvironment env, short layers) {
        this.env = env;
        this.parallelism = (short) this.env.getParallelism();
        this.layers = layers;
//        this.env.setStateBackend(new EmbeddedRocksDBStateBackend());
        this.env.getConfig().setAutoWatermarkInterval(5000);
        this.env.getConfig().enableObjectReuse(); // Optimization
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
     * @param inputData non-keyed input graphop to this layer, can be a union of several streams as well
     * @param processFunction ProcessFunction for this operator at this layer
     * @return output stream dependent on the plugin
     */
    public SingleOutputStreamOperator<GraphOp> streamingGNNLayer(DataStream<GraphOp> inputData, KeyedProcessFunction<String, GraphOp, GraphOp> processFunction) {
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers - 1)));
        IterationID localIterationId = new IterationID();

        KeyedStream<GraphOp, String> keyedLast = inputData
                .keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> forward = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism);

        if(position_index > 0){
            // Add iteration
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform("IterationTail", TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism);
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        }

        forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index > 1) {
            // Add Backward Iteration
            int previousParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 2, this.layers)));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism);
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
     * @param processFunctions   List of Storages with corresponding plugins
     * @return Last layer corresponding to vertex embeddings
     */
    public SingleOutputStreamOperator<GraphOp> gnnEmbeddings(BiFunction<DataStream<GraphOp>, KeyedProcessFunction<String, GraphOp, GraphOp>, SingleOutputStreamOperator<GraphOp>> fn, DataStream<GraphOp> allUpdates, List<KeyedProcessFunction<String, GraphOp, GraphOp>> processFunctions) {
        assert layers > 0;
        assert position_index == 0;
        DataStream<GraphOp> topologyUpdates = null;
        DataStream<GraphOp> normalUpdates = null;
        DataStream<GraphOp> trainTestSplit = null;
        DataStream<GraphOp> previousLayerUpdates = null;

        for (KeyedProcessFunction processFn : processFunctions) {
            if(position_index == 0){
                SingleOutputStreamOperator<GraphOp> tmp = fn.apply(allUpdates, processFn);
                topologyUpdates = tmp.getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
                trainTestSplit = tmp.getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);
                normalUpdates = tmp;
            }
            else if(position_index == 1){
                previousLayerUpdates = fn.apply(normalUpdates, processFn);
            }
            else if(position_index <= layers){
                // Still mid layer
                previousLayerUpdates = fn.apply(previousLayerUpdates.union(topologyUpdates), processFn);
            }
            else{
                previousLayerUpdates = fn.apply(previousLayerUpdates.union(trainTestSplit), processFn);
            }

        }
        return (SingleOutputStreamOperator<GraphOp>) previousLayerUpdates;
    }


    /**
     * Helper function to add a new layer of GNN Iteration, explicitly used to trainer. Otherwise chain starts from @gnnEmbeddings()
     *
     * @param topologyUpdates non-keyed input graphop to this layer, can be a union of several streams as well
     * @param storage         Storage of choice with attached plugins
     * @return output stream dependent on the plugin
     */
    @Deprecated
    public SingleOutputStreamOperator<GraphOp> windowingGNNLayer(DataStream<GraphOp> topologyUpdates, BaseStorage storage) {
        int thisParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 1, this.layers - 1)));
        IterationID localIterationId = new IterationID();

        // Create a window Operator
        KeyedStream<GraphOp, String> keyedLast = topologyUpdates
                .keyBy(new PartKeySelector());
        ListStateDescriptor<GraphOp> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents", TypeInformation.of(GraphOp.class).createSerializer(env.getConfig()));
        WindowAssigner<Object, TimeWindow> assigner = TumblingEventTimeWindows.of(Time.seconds(2));
        WindowOperator<String, GraphOp, Iterable<GraphOp>, GraphOp, TimeWindow> s = new WindowOperator<String, GraphOp, Iterable<GraphOp>, GraphOp, TimeWindow>(
                assigner,
                assigner.getWindowSerializer(env.getConfig()),
                keyedLast.getKeySelector(),
                keyedLast.getKeyType().createSerializer(env.getConfig()),
                stateDesc,
                new WindowingGNNLayerFunction(storage),
                assigner.getDefaultTrigger(env),
                Long.MAX_VALUE,
                null);

        // Wrap the operator

        SingleOutputStreamOperator<GraphOp> forward = keyedLast.transform("Gnn Operator", TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(s, localIterationId, position_index, layers)).setParallelism(thisParallelism);
        SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform("IterationTail", TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism);

        forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index > 1) {
            int previousParallelism = (int) (parallelism * Math.pow(this.lambda, Math.min(this.position_index - 2, this.layers)));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism);
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;

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
        DataStream<Void> backIteration = losses.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(trainingStream.getParallelism());
        backIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        return losses;
    }


}
