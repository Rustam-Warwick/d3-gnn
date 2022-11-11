package helpers;

import ai.djl.ndarray.NDHelper;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.serializers.NDArrayRawSerializer;
import datasets.Dataset;
import elements.*;
import features.MeanAggregator;
import features.Parts;
import features.Tensor;
import functions.selectors.PartKeySelector;
import operators.BaseWrapperOperator;
import operators.IterationTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import partitioner.BasePartitioner;

import java.util.Arrays;

public class GraphStream {

    private final StreamExecutionEnvironment env; // Stream environment

    private final String[] cmdArgs; // Command Line Arguments for the job

    public double lambda = 1; // GNN operator explosion coefficient. 1 means no explosion

    private boolean fineGrainedResourceManagementEnabled = false; // Add custom slotSharingGroupsForOperators

    private String partitionerName = "random"; // Partitioner Name

    private String dataset = "cora"; // Dataset to process

    private short layers;// Number of GNN Layers in the pipeline

    private short position_index; // Counter of the Current GNN layer being generated

    private IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending

    private IterationID fullLoopIterationId; // Iteration Id of 0 layer

    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs) {
        Arrays.sort(cmdArgs);
        this.env = env;
        this.cmdArgs = cmdArgs;
        configureSerializers(env);
        parseCmdArgs(); // Global job parameter arguments need to be parsed
    }

    public static void configureSerializers(StreamExecutionEnvironment env) {
        NDHelper.addSerializers(env.getConfig());
        env.registerType(GraphElement.class);
        env.registerType(ReplicableGraphElement.class);
        env.registerType(Vertex.class);
        env.registerType(DEdge.class);
        env.registerType(Feature.class);
        env.registerType(Parts.class);
        env.registerType(Tensor.class);
        env.registerType(Rmi.class);
        env.registerType(MeanAggregator.class);
        env.registerType(PartNumber.class);
    }

    public String getPartitionerName() {
        return partitionerName;
    }

    public String getDataset() {
        return dataset;
    }

    private void parseCmdArgs() {
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

        Option dataset = Option
                .builder("d")
                .longOpt("dataset")
                .required(false)
                .desc("Partitioner type")
                .type(String.class)
                .hasArg(true)
                .argName("value")
                .numberOfArgs(1)
                .build();

        Option fineGrainedResources = Option
                .builder("f")
                .longOpt("fineGrainedResource")
                .required(false)
                .desc("Fine Grained Resource Management Enabled")
                .build();

        Option tensorCompression = Option
                .builder("dc")
                .longOpt("disableCompression")
                .required(false)
                .desc("Tensor Compression Disabled")
                .build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        options.addOption(explosionCoeff);
        options.addOption(dataset);
        options.addOption(objectReuse);
        options.addOption(partitioner);
        options.addOption(fineGrainedResources);
        options.addOption(tensorCompression);
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
            if (commandLine.hasOption("o")) {
                this.env.getConfig().enableObjectReuse();
            }
            if (commandLine.hasOption("d")) {
                this.dataset = commandLine.getOptionValue("d");
            }
            if (commandLine.hasOption("f")) {
                this.fineGrainedResourceManagementEnabled = !(env instanceof LocalStreamEnvironment);
            }
            if (commandLine.hasOption("dc")) {
                env.registerTypeWithKryoSerializer(PtNDArray.class, NDArrayRawSerializer.class);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Partition the incoming GraphOp Stream into getMaxParallelism() number of parts
     *
     * @param stream Incoming GraphOp Stream
     * @return Partitioned but not-keyed DataStream of GraphOps.
     */
    protected DataStream<GraphOp> partition(DataStream<GraphOp> stream) {
        BasePartitioner partitioner = BasePartitioner.getPartitioner(partitionerName).parseCmdArgs(cmdArgs).setPartitions((short) env.getMaxParallelism());
        SingleOutputStreamOperator<GraphOp> partitionedOutput = partitioner.partition(stream, fineGrainedResourceManagementEnabled);
        return partitionedOutput;
    }

    protected SingleOutputStreamOperator<GraphOp> streamingGNNLayerAsSource(DataStream<GraphOp> inputData, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction, boolean hasBackwardIteration, boolean hasFoolLoopIteration) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(position_index - 1, 0)));
        IterationID localIterationId = new IterationID();
        SingleOutputStreamOperator<GraphOp> forward;

        if (position_index > 0 || hasFoolLoopIteration) {
            // Add the iteration heads
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            forward.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
            if (fineGrainedResourceManagementEnabled) {
                forward.slotSharingGroup("gnn-" + Math.max(position_index - 1, 0)); // position 0 and 1 in same slot sharing group preferably
            }
            if (position_index == 0) {
                // This was Splitter Full-Loop iteration
                fullLoopIterationId = localIterationId;
            }
        } else {
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            if (fineGrainedResourceManagementEnabled)
                forward.slotSharingGroup("gnn-" + Math.max(position_index - 1, 0));
        }

        if (position_index > 0) {
            // Iteration Tails
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform(String.format("IterationTail - %s", position_index), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism).uid(String.format("IterationTail - %s", position_index));
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + position_index);
            if (fineGrainedResourceManagementEnabled)
                iterationHandler.slotSharingGroup("gnn-" + Math.max(position_index - 1, 0));
        }

        if (position_index > 1 && hasBackwardIteration) {
            // Add Backward Iteration
            int previousParallelism = (int) (env.getParallelism() * Math.pow(lambda, position_index - 2));
            SingleOutputStreamOperator<Void> backwardIteration = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()).transform(String.format("BackwardTail - %s", position_index - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism).uid(String.format("BackwardTail - %s", position_index - 1));
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (position_index - 1));
            if (fineGrainedResourceManagementEnabled)
                backwardIteration.slotSharingGroup("gnn-" + Math.max(position_index - 2, 0));
        }
        if (position_index == layers && hasFoolLoopIteration) {
            // Add Full Loop Iteration
            SingleOutputStreamOperator<Void> fullLoopIteration = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.fullLoopIterationId)).setParallelism(env.getParallelism()).uid("FullLoopTail");
            fullLoopIteration.getTransformation().setCoLocationGroupKey("gnn-0");
            if (fineGrainedResourceManagementEnabled) fullLoopIteration.slotSharingGroup("gnn-0");
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;
    }

    /**
     * Main method for invoking the GNN Chain
     *
     * @param dataStreamMain       External System queries datastream.
     * @param processFunctions     List of Storages with corresponding plugins. It is assumed that the first one is a process that splits the data into several parts, TRAINING, TESTING, TOPOLOGY only and etc.
     * @param hasBackwardIteration Should backward iterations exist in the GNN Chain. Backward iterations are useful if we are doing backprop in the graph
     * @param hasLastLayerTopology Should the last layer receive full topology or no. Useful if we are doing some kind of autoregressive model.
     * @return L+1 outputs corresponding to (Partitioned data, ... output of all the processFunctions)
     * @implNote First Process function will be replayable, and last one will be output with connection to first one(if FullLoopIteration is enabled)
     */
    protected DataStream<GraphOp>[] gnnEmbeddings(DataStream<GraphOp> dataStreamMain, boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>... processFunctions) {
        // 1. intilialize the variables
        assert layers == 0; // Untouched before
        assert position_index == 0; // Untouched before
        this.layers = (short) (processFunctions.length - 1); // First input is not counted as a layer
        DataStream<GraphOp>[] layerOutputs = new DataStream[processFunctions.length + 1]; // the final return value
        DataStream<GraphOp> topologyUpdates = null;
        DataStream<GraphOp> trainTestSplit = null;
        env.setMaxParallelism((int) (env.getParallelism() * Math.pow(lambda, layers - 1)));

        // 2. Partitiong the incoming stream
//        SingleOutputStreamOperator<GraphOp> previousLayerUpdates = ((SingleOutputStreamOperator<GraphOp>) partition(dataStreamMain)).disableChaining().process(new Throttler(500, 50000)).name("Throttler").setParallelism(1);
        SingleOutputStreamOperator<GraphOp> previousLayerUpdates = ((SingleOutputStreamOperator<GraphOp>) partition(dataStreamMain));
        layerOutputs[0] = previousLayerUpdates; // First one is the partitioned data

        // 3. Execute the layers
        for (int i = 0; i <= layers; i++) {
            KeyedProcessFunction processFn = processFunctions[i];
            assert processFn != null;
            if (i == 0) {
                previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates, processFn, hasBackwardIteration, hasFullLoopIteration);
                topologyUpdates = previousLayerUpdates.getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
                trainTestSplit = previousLayerUpdates.getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);
            } else if (i == 1) {
                previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates, processFn, hasBackwardIteration, hasFullLoopIteration);
            } else if (i < layers) {
                previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(topologyUpdates), processFn, hasBackwardIteration, hasFullLoopIteration);
            } else {
                if (hasLastLayerTopology && hasBackwardIteration)
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(topologyUpdates, trainTestSplit), processFn, hasBackwardIteration, hasFullLoopIteration);
                else if (hasLastLayerTopology) {
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(topologyUpdates), processFn, hasBackwardIteration, hasFullLoopIteration);
                } else if (hasBackwardIteration) {
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(trainTestSplit), processFn, hasBackwardIteration, hasFullLoopIteration);
                } else {
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates, processFn, hasBackwardIteration, hasFullLoopIteration);
                }
            }
            layerOutputs[i + 1] = previousLayerUpdates;
        }
        return layerOutputs;
    }

    /**
     * Wrapper that enfoernces the use of Dataset from the dataset name
     */
    public DataStream<GraphOp>[] gnnEmbeddings(boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>... processFunctions) {
        Dataset concreteDataset = Dataset.getDataset(dataset).parseCmdArgs(cmdArgs);
        DataStream<GraphOp> dataStreamMain = concreteDataset.build(env, fineGrainedResourceManagementEnabled);
        KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctionAndTrainTest = new KeyedProcessFunction[processFunctions.length + 1];
        processFunctionAndTrainTest[0] = concreteDataset.trainTestSplitter();
        System.arraycopy(processFunctions, 0, processFunctionAndTrainTest, 1, processFunctions.length);
        return gnnEmbeddings(dataStreamMain, hasLastLayerTopology, hasBackwardIteration, hasFullLoopIteration, processFunctionAndTrainTest);
    }
}
