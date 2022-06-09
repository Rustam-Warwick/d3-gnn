package helpers;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.nn.Parameter;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import ai.djl.serializers.NDArraySerializer;
import ai.djl.serializers.NDManagerSerializer;
import ai.djl.serializers.ParameterSerializer;
import datasets.Dataset;
import elements.*;
import elements.iterations.Rmi;
import features.Set;
import features.VTensor;
import functions.selectors.PartKeySelector;
import operators.BaseWrapperOperator;
import operators.IterationHeadOperator;
import operators.IterationTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import partitioner.BasePartitioner;

import java.util.Objects;

public class GraphStream {
    public final short parallelism; // Default parallelism

    public final StreamExecutionEnvironment env; // Stream environment

    private final boolean isLocal;

    public double lambda; // GNN operator explosion coefficient. 1 means no explosion

    public short layers;// Number of GNN Layers in the pipeline

    public String partitionerName = "random";

    public IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending

    public IterationID fullLoopIterationId; // Iteration Id of 0 layer

    public short position_index; // Counter of the Current GNN layer being

    public GraphStream(StreamExecutionEnvironment env, double explosionFactor) {
        this.env = env;
        this.parallelism = (short) this.env.getParallelism();
        this.lambda = explosionFactor;
        configureSerializers(this.env);
        if (env instanceof LocalStreamEnvironment) {
            isLocal = true;
        } else isLocal = false;
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

    public GraphStream parseCmdArgs(String[] cmdArgs) {
        Option explosionCoeff = Option
                .builder("l")
                .required(false)
                .desc("Explosion Coefficient of the GNN models")
                .type(Float.class)
                .hasArg(true)
                .argName("Value")
                .longOpt("lambda")
                .numberOfArgs(1)
                .build();

        Option objectReuse = Option
                .builder("o")
                .longOpt("objectReuse")
                .required(false)
                .desc("Enable object reuse")
                .hasArg(false)
                .build();

        Option partitioner = Option
                .builder("p")
                .longOpt("partitioner")
                .required(false)
                .desc("Partitioner type")
                .type(String.class)
                .hasArg(true)
                .argName("value")
                .numberOfArgs(1)
                .build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        options.addOption(explosionCoeff);
        options.addOption(objectReuse);
        options.addOption(partitioner);
        try {
            CommandLine commandLine = parser.parse(options, cmdArgs);
            if (commandLine.hasOption("l")) {
                String lambdaValue = commandLine.getOptionValue("lambda");
                double r = Double.valueOf(lambdaValue);
                this.lambda = r;
            }
            if (commandLine.hasOption("p")) {
                String lambdaValue = commandLine.getOptionValue("p");
                this.partitionerName = lambdaValue;
            }
            if(commandLine.hasOption("o")){
                this.env.getConfig().enableObjectReuse();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Partition the incoming GraphOp Stream into getMaxParallelism() number of parts
     *
     * @param stream      Incoming GraphOp Stream
     * @return Partitioned but not-keyed DataStream of GraphOps.
     */
    public DataStream<GraphOp> partition(DataStream<GraphOp> stream) {
        BasePartitioner partitioner = BasePartitioner.getPartitioner(partitionerName);
        partitioner.partitions = (short) this.env.getMaxParallelism();
        short part_parallelism = this.parallelism;
        if (!partitioner.isParallel()) part_parallelism = 1;
        return stream.map(partitioner).setParallelism(part_parallelism).name(partitioner.getName());
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

        SingleOutputStreamOperator<GraphOp> iterationHead = inputData.transform(String.format("IterationHead - %s", position_index), TypeInformation.of(GraphOp.class), new IterationHeadOperator(localIterationId, position_index)).uid(String.format("IterationHead - %s", position_index)).setParallelism(thisParallelism);
        SingleOutputStreamOperator<GraphOp> forward = iterationHead.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));

//        iterationHead.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
//        forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
        if (!isLocal) forward.slotSharingGroup("gnn-" + position_index);
        if (!isLocal) iterationHead.slotSharingGroup("gnn-" + position_index);

        if (position_index == 0) {
            fullLoopIterationId = localIterationId;
        }

        if (position_index > 0) {
            // Add iteration
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).forward().transform(String.format("IterationTail - %s", position_index), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism).uid(String.format("IterationTail - %s", position_index));
//            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
            if (!isLocal) iterationHandler.slotSharingGroup("gnn-" + position_index);
        }


        if (position_index > 1) {
            // Add Backward Iteration
            int previousParallelism = (int) (parallelism * Math.pow(lambda, position_index - 1));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.transform(String.format("BackwardTail - %s", position_index - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism).uid(String.format("BackwardTail - %s", position_index - 1));
//            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
            if (!isLocal) backwardIteration.slotSharingGroup("gnn-" + (position_index - 1));
        }
        if (position_index == layers) {
            // Add Full Loop Iteration
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.fullLoopIterationId)).setParallelism(parallelism).uid("FullLoopTail");
//            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-0");
            if (!isLocal) backwardIteration.slotSharingGroup("gnn-0");

        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;
    }

    /**
     * Start of the GNN Chain
     * @implNote First Process function will be replayable, and last one will be output with connection to first one(FullLoopIteration)
     * @param allUpdates       External System updates
     * @param processFunctions List of Storages with corresponding plugins
     * @return Last layer corresponding to vertex embeddings
     */
    public SingleOutputStreamOperator<GraphOp> gnnEmbeddings(DataStream<GraphOp> allUpdates, KeyedProcessFunction<String, GraphOp, GraphOp> ...processFunctions) {
        assert layers == 0;
        assert position_index == 0;
        this.layers = (short) (processFunctions.length - 1); // First input is not counted as a layer
        DataStream<GraphOp> topologyUpdates = null;
        DataStream<GraphOp> normalUpdates = null;
        DataStream<GraphOp> trainTestSplit = null;
        DataStream<GraphOp> previousLayerUpdates = null;
        for (KeyedProcessFunction processFn : processFunctions) {
            if(Objects.isNull(processFn))continue;
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
