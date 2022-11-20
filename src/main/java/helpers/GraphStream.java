package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import features.MeanAggregator;
import features.Parts;
import features.Tensor;
import functions.selectors.PartKeySelector;
import operators.BaseWrapperOperator;
import operators.IterationTailOperator;
import operators.WrapperOperatorFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationID;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.util.Preconditions;
import partitioner.BasePartitioner;
import picocli.CommandLine;

import java.util.Arrays;

public class GraphStream {

    protected final StreamExecutionEnvironment env; // Stream environment

    protected final KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctions; // GNN Process Functions

    protected final boolean hasLastLayerTopology; // Send topology updates to the last layer

    protected final boolean hasBackwardIteration;

    protected final boolean hasFullLoopIteration;

    protected final Dataset datasetInstance;

    protected final BasePartitioner partitionerInstance;

    protected final short layers; // Number of GNN Layers in the pipeline

    @CommandLine.Option(names = {"-l", "--lambda"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = "explosion coefficient")
    public double lambda; // GNN operator explosion coefficient. 1 means no explosion

    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled; // Add custom slotSharingGroupsForOperators

    @CommandLine.Option(names = {"-p", "--partitioner"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Partitioner to be used")
    protected String partitioner;

    @CommandLine.Option(names = {"-d", "--dataset"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Dataset to be used")
    protected String dataset;

    protected short position_index; // Counter of the Current GNN layer being generated

    protected IterationID lastIterationID; // Previous Layer Iteration Id used for backward message sending

    protected IterationID fullLoopIterationId; // Iteration Id of 0 layer

    @SafeVarargs
    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>... processFunctions) {
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.configureSerializers();
        this.hasFullLoopIteration = hasFullLoopIteration;
        this.hasBackwardIteration = hasBackwardIteration;
        this.hasLastLayerTopology = hasLastLayerTopology;
        this.processFunctions = processFunctions;
        this.layers = (short) processFunctions.length;
        this.datasetInstance = Dataset.getDataset(dataset, cmdArgs);
        this.partitionerInstance = BasePartitioner.getPartitioner(partitioner, cmdArgs);
        env.setMaxParallelism((int) (env.getParallelism() * Math.pow(lambda, layers - 1)));
    }

    /**
     * Helper method for configuring necessary serializers for Flink
     */
    public void configureSerializers() {
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

    /**
     * Helper method for creating single GNN Layer
     */
    protected SingleOutputStreamOperator<GraphOp> streamingStorageLayer(DataStream<GraphOp> inputData, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(position_index - 1, 0)));
        IterationID localIterationId = new IterationID();
        SingleOutputStreamOperator<GraphOp> forward;
        if (position_index > 0 || hasFullLoopIteration) {
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
        if (position_index == layers && hasFullLoopIteration) {
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
     */
    protected DataStream<GraphOp>[] build() {
        Preconditions.checkNotNull(datasetInstance);
        Preconditions.checkNotNull(partitionerInstance);
        if(position_index != 0) throw new IllegalStateException("Cannot call this method twice");
        SingleOutputStreamOperator<GraphOp>[] layerOutputs = new SingleOutputStreamOperator[layers + 3]; // the final return value
        layerOutputs[0] = (SingleOutputStreamOperator<GraphOp>) datasetInstance.build(env);
        layerOutputs[1] = partitionerInstance.setPartitions((short) env.getMaxParallelism()).partition(layerOutputs[0]);
        layerOutputs[2] = streamingStorageLayer(layerOutputs[1], datasetInstance.trainTestSplitter());

        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);

        for (int i = 0; i < layers; i++) {
            KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFn = processFunctions[i];
            if(i == 0){
                layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2], processFn);
            }
            else if (i == layers - 1) {
                if(hasLastLayerTopology) layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(topologyUpdates, trainTestSplit), processFn);
                else layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(trainTestSplit), processFn);
            } else {
                layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(topologyUpdates), processFn);
            }
        }
        return layerOutputs;
    }

}
