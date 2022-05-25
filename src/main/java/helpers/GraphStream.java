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
import functions.selectors.PartKeySelector;
import operators.BaseWrapperOperator;
import operators.IterationTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.objenesis.strategy.StdInstantiatorStrategy;
import partitioner.BasePartitioner;

import java.util.List;

public class GraphStream {
    public final short parallelism; // Default parallelism
    public final double lambda; // GNN operator explosion coefficient. 1 means no explosion
    public final StreamExecutionEnvironment env; // Stream environment
    public short layers;// Number of GNN Layers in the pipeline
    public IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending
    public IterationID fullLoopIterationId; // Iteration Id of 0 layer
    public short position_index; // Counter of the Current GNN layer being

    public GraphStream(StreamExecutionEnvironment env, double explosionFactor) {
        this.env = env;
        this.parallelism = (short) this.env.getParallelism();
        this.lambda = explosionFactor;
        configureSerializers(this.env);
    }

    public GraphStream(StreamExecutionEnvironment env) {
        this(env, 1);
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
     * @param inputData       non-keyed input graphop to this layer, can be a union of several streams as well
     * @param processFunction ProcessFunction for this operator at this layer
     * @return output stream dependent on the plugin
     */
    public SingleOutputStreamOperator<GraphOp> streamingGNNLayer(DataStream<GraphOp> inputData, KeyedProcessFunction<String, GraphOp, GraphOp> processFunction) {
        int thisParallelism = (int) (parallelism * Math.pow(lambda, position_index));
        IterationID localIterationId = new IterationID();

        KeyedStream<GraphOp, String> keyedLast = inputData
                .keyBy(new PartKeySelector());
        SingleOutputStreamOperator<GraphOp> forward = keyedLast.transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism);

        forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);

        if (position_index == 0) {
            fullLoopIterationId = localIterationId;
        }

        if (position_index > 0) {
            // Add iteration
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform(String.format("IterationTail - %s", position_index), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism);
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        }


        if (position_index > 1) {
            // Add Backward Iteration
            int previousParallelism = (int) (parallelism * Math.pow(lambda, position_index - 1));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform(String.format("BackwardTail - %s", position_index - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism);
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        }
        if (position_index == layers) {
            // This is the last one
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.keyBy(new PartKeySelector()).transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.fullLoopIterationId)).setParallelism(parallelism);
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-0");

        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;
    }

    /**
     * Start of the GNN Chain
     *
     * @param allUpdates       External System updates
     * @param processFunctions List of Storages with corresponding plugins
     * @return Last layer corresponding to vertex embeddings
     */
    public SingleOutputStreamOperator<GraphOp> gnnEmbeddings(DataStream<GraphOp> allUpdates, List<KeyedProcessFunction<String, GraphOp, GraphOp>> processFunctions) {
        assert layers == 0;
        assert position_index == 0;
        this.layers = (short) (processFunctions.size() - 1); // First input is not counted as a layer
        DataStream<GraphOp> topologyUpdates = null;
        DataStream<GraphOp> normalUpdates = null;
        DataStream<GraphOp> trainTestSplit = null;
        DataStream<GraphOp> previousLayerUpdates = null;
        for (KeyedProcessFunction processFn : processFunctions) {
            if (position_index == 0) {
                SingleOutputStreamOperator<GraphOp> tmp = streamingGNNLayer(allUpdates, processFn);
                topologyUpdates = tmp.getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
                trainTestSplit = tmp.getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);
                normalUpdates = tmp;
            } else if (position_index == 1) {
                previousLayerUpdates = streamingGNNLayer(normalUpdates, processFn);
            } else if (position_index < layers) {
                // Still mid layer
                previousLayerUpdates = streamingGNNLayer(previousLayerUpdates.union(topologyUpdates), processFn);
            } else {
                previousLayerUpdates = streamingGNNLayer(previousLayerUpdates.union(trainTestSplit), processFn);
            }

        }
        return (SingleOutputStreamOperator<GraphOp>) previousLayerUpdates;
    }

    public DataStream<GraphOp> gnnLoss(DataStream<GraphOp> trainingStream, ProcessFunction<GraphOp, GraphOp> lossFunction) {
        DataStream<GraphOp> losses = trainingStream.process(lossFunction).setParallelism(1);
        DataStream<Void> backIteration = losses.keyBy(new PartKeySelector()).transform("BackwardTail", TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(trainingStream.getParallelism());
        backIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
        return losses;
    }


}
