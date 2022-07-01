package helpers;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDHelper;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.serializers.NDArrayRawSerializer;
import datasets.Dataset;
import elements.*;
import elements.iterations.Rmi;
import features.Set;
import features.Tensor;
import functions.selectors.PartKeySelector;
import operators.*;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import partitioner.BasePartitioner;

import java.util.Objects;

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
        this.env = env;
        this.cmdArgs = cmdArgs;
        configureSerializers();
        parseCmdArgs();
    }

    public boolean isFineGrainedResourceManagementEnabled() {
        return fineGrainedResourceManagementEnabled;
    }

    public String getPartitionerName() {
        return partitionerName;
    }

    public String getDataset() {
        return dataset;
    }

    private void configureSerializers() {
        NDHelper.addSerializers(env.getConfig());
        env.registerType(GraphElement.class);
        env.registerType(ReplicableGraphElement.class);
        env.registerType(Vertex.class);
        env.registerType(Edge.class);
        env.registerType(Feature.class);
        env.registerType(Set.class);
        env.registerType(Tensor.class);
        env.registerType(BaseAggregator.class);
        env.registerType(Rmi.class);
        env.registerType(MeanAggregator.class);
        env.registerType(PartNumber.class);
    }

    public void parseCmdArgs() {
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
        BasePartitioner partitioner = BasePartitioner.getPartitioner(partitionerName);
        partitioner.parseCmdArgs(cmdArgs);
        partitioner.partitions = (short) env.getMaxParallelism();
        SingleOutputStreamOperator<GraphOp> partitionedOutput = partitioner.partition(stream, fineGrainedResourceManagementEnabled);
        return partitionedOutput;
    }


    /**
     * Helper function to add a new layer of GNN Iteration, explicitly used to trainer. Otherwise chain starts from @gnnEmbeddings()
     *
     * @param inputData       non-keyed input graphop to this layer, can be a union of several streams as well
     * @param processFunction ProcessFunction for this operator at this layer
     * @return output stream dependent on the plugin
     */
    protected SingleOutputStreamOperator<GraphOp> streamingGNNLayer(DataStream<GraphOp> inputData, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction, boolean hasBackwardIteration, boolean hasFoolLoopIteration) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(position_index - 1, 0)));
        IterationID localIterationId = new IterationID();
        SingleOutputStreamOperator<GraphOp> forward;
//        if (fineGrainedResourceManagementEnabled) {
//            env.registerSlotSharingGroup(SlotSharingGroup
//                    .newBuilder("gnn-" + thisParallelism)
//                    .setTaskHeapMemoryMB(600)
//                    .setTaskOffHeapMemoryMB(700)
//                    .setCpuCores(1)
//                    .build());
//        }

        if (position_index > 0 || hasFoolLoopIteration) {
            // Iteration Heads should always exist here
            SingleOutputStreamOperator<GraphOp> iterationHead = inputData.transform(String.format("IterationHead - %s", position_index), TypeInformation.of(GraphOp.class), new IterationHeadOperator(localIterationId, position_index)).uid(String.format("IterationHead - %s", position_index)).setParallelism(thisParallelism);
            forward = iterationHead.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            iterationHead.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            forward.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled) forward.slotSharingGroup("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled) iterationHead.slotSharingGroup("gnn-" + thisParallelism);
        } else {
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            if (fineGrainedResourceManagementEnabled) forward.slotSharingGroup("gnn-" + thisParallelism);
        }

        if (position_index == 0 && hasFoolLoopIteration) {
            fullLoopIterationId = localIterationId;
        }

        if (position_index > 0) {
            // Add iteration, these are always added
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).forward().transform(String.format("IterationTail - %s", position_index), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism).uid(String.format("IterationTail - %s", position_index));
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled) iterationHandler.slotSharingGroup("gnn-" + thisParallelism);
        }

        if (position_index > 1 && hasBackwardIteration) {
            // Add Backward Iteration
            int previousParallelism = (int) (env.getParallelism() * Math.pow(lambda, position_index - 2));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.transform(String.format("BackwardTail - %s", position_index - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism).uid(String.format("BackwardTail - %s", position_index - 1));
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + previousParallelism);
            if (fineGrainedResourceManagementEnabled) backwardIteration.slotSharingGroup("gnn-" + previousParallelism);
        }
        if (position_index == layers && hasFoolLoopIteration) {
            // Add Full Loop Iteration
            DataStream<GraphOp> fullLoopFilter = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = fullLoopFilter.transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.fullLoopIterationId)).setParallelism(env.getParallelism()).uid("FullLoopTail");
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + env.getParallelism());
            if (fineGrainedResourceManagementEnabled) backwardIteration.slotSharingGroup("gnn-" + env.getParallelism());
        }
        this.position_index++;
        this.lastIterationID = localIterationId;
        return forward;
    }

    protected SingleOutputStreamOperator<GraphOp> streamingGNNLayerAsSource(DataStream<GraphOp> inputData, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction, boolean hasBackwardIteration, boolean hasFoolLoopIteration) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(position_index - 1, 0)));
        IterationID localIterationId = new IterationID();
        SingleOutputStreamOperator<GraphOp> forward;
//        if (fineGrainedResourceManagementEnabled) {
//            env.registerSlotSharingGroup(SlotSharingGroup
//                    .newBuilder("gnn-" + thisParallelism)
//                    .setTaskHeapMemoryMB(600)
//                    .setTaskOffHeapMemoryMB(700)
//                    .setCpuCores(1)
//                    .build());
//        }

        if (position_index > 0 || hasFoolLoopIteration) {
            // Iteration Heads should always exist here
            DataStreamSource<GraphOp> iterationHead = new DataStreamSource<>(env, TypeInformation.of(GraphOp.class), new IterationSourceOperator(localIterationId, position_index), true, String.format("IterationHead - %s", position_index), Boundedness.BOUNDED).setParallelism(thisParallelism);
            forward = inputData.union(iterationHead).keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            iterationHead.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            forward.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled) forward.slotSharingGroup("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled)
                iterationHead.getTransformation().setSlotSharingGroup("gnn-" + thisParallelism);
        } else {
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position_index), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, position_index, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", position_index));
            if (fineGrainedResourceManagementEnabled) forward.slotSharingGroup("gnn-" + thisParallelism);
        }

        if (position_index == 0 && hasFoolLoopIteration) {
            fullLoopIterationId = localIterationId;
        }

        if (position_index > 0) {
            // Add iteration, these are always added
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).forward().transform(String.format("IterationTail - %s", position_index), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism).uid(String.format("IterationTail - %s", position_index));
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + thisParallelism);
            if (fineGrainedResourceManagementEnabled) iterationHandler.slotSharingGroup("gnn-" + thisParallelism);
        }

        if (position_index > 1 && hasBackwardIteration) {
            // Add Backward Iteration
            int previousParallelism = (int) (env.getParallelism() * Math.pow(lambda, position_index - 2));
            DataStream<GraphOp> backFilter = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = backFilter.transform(String.format("BackwardTail - %s", position_index - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.lastIterationID)).setParallelism(previousParallelism).uid(String.format("BackwardTail - %s", position_index - 1));
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + previousParallelism);
            if (fineGrainedResourceManagementEnabled) backwardIteration.slotSharingGroup("gnn-" + previousParallelism);
        }
        if (position_index == layers && hasFoolLoopIteration) {
            // Add Full Loop Iteration
            DataStream<GraphOp> fullLoopFilter = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG);
            SingleOutputStreamOperator<Void> backwardIteration = fullLoopFilter.transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.fullLoopIterationId)).setParallelism(env.getParallelism()).uid("FullLoopTail");
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + env.getParallelism());
            if (fineGrainedResourceManagementEnabled) backwardIteration.slotSharingGroup("gnn-" + env.getParallelism());
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
        SingleOutputStreamOperator<GraphOp> previousLayerUpdates = null;
        env.setMaxParallelism((int) (env.getParallelism() * Math.pow(lambda, layers - 1)));

        // 2. Partitiong the incoming stream
        DataStream<GraphOp> allUpdates = partition(dataStreamMain);
        layerOutputs[0] = allUpdates; // First one is the partitioned data

        // 3. Execute the layers
        for (int i = 0; i <= layers; i++) {
            KeyedProcessFunction processFn = processFunctions[i];
            if (Objects.isNull(processFn)) {
                layerOutputs[i + 1] = null;
                continue;
            }
            if (position_index == 0) {
                previousLayerUpdates = streamingGNNLayerAsSource(allUpdates, processFn, hasBackwardIteration, hasFullLoopIteration);
                topologyUpdates = previousLayerUpdates.getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
                trainTestSplit = previousLayerUpdates.getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);
            } else if (position_index == 1) {
                previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates, processFn, hasBackwardIteration, hasFullLoopIteration);
            } else if (position_index < layers) {
                previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(topologyUpdates), processFn, hasBackwardIteration, hasFullLoopIteration);
            } else {
                if (hasLastLayerTopology)
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(trainTestSplit, topologyUpdates), processFn, hasBackwardIteration, hasFullLoopIteration);
                else
                    previousLayerUpdates = streamingGNNLayerAsSource(previousLayerUpdates.union(trainTestSplit), processFn, hasBackwardIteration, hasFullLoopIteration);
            }
            layerOutputs[i + 1] = previousLayerUpdates;
        }
        return layerOutputs;
    }

    /**
     * Wrapper that enfoernces the use of Dataset from the dataset name
     */
    public DataStream<GraphOp>[] gnnEmbeddings(boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>... processFunctions) {
        Dataset concreteDataset = Dataset.getDataset(dataset);
        DataStream<GraphOp> dataStreamMain = concreteDataset.build(env, fineGrainedResourceManagementEnabled);
        KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctionAndTrainTest = new KeyedProcessFunction[processFunctions.length + 1];
        processFunctionAndTrainTest[0] = concreteDataset.trainTestSplitter();
        System.arraycopy(processFunctions, 0, processFunctionAndTrainTest, 1, processFunctions.length);
        return gnnEmbeddings(dataStreamMain, hasLastLayerTopology, hasBackwardIteration, hasFullLoopIteration, processFunctionAndTrainTest);
    }
}
