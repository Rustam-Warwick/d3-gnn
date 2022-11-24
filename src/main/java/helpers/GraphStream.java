package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import features.MeanAggregator;
import features.Parts;
import features.Tensor;
import functions.selectors.PartKeySelector;
import functions.storage.StorageProcessFunction;
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
import partitioner.Partitioner;
import picocli.CommandLine;

import java.util.Arrays;

/**
 * Helper class for creating a pipeline
 * <p>
 * Pipeline usually starts with:
 * {@link Dataset} --> {@link Partitioner} --> Splitter --> Storage-0 --> ... --> Storage-N
 * It also includes {@link IterationTailOperator} for iterative messages
 * </p>
 */
public class GraphStream {

    /**
     * Execution environment
     */
    protected final StreamExecutionEnvironment env;

    /**
     * List of {@link StorageProcessFunction}
     */
    protected final KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctions;

    /**
     * If the last storage layer should receive topology updates
     */
    protected final boolean hasLastLayerTopology;

    /**
     * If backward iterations should be added
     */
    protected final boolean hasBackwardIteration;

    /**
     * If last Storage layer and splitter should have a connection
     */
    protected final boolean hasFullLoopIteration;

    /**
     * {@link Partitioner} to be used
     */
    protected final Partitioner partitioner;

    /**
     * Number of Storage layers in the pipeline {@code processFunctions.length}
     */
    protected final short layers;

    /**
     * Explosion coefficient across the Storage layers
     */
    @CommandLine.Option(names = {"-l", "--lambda"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = "explosion coefficient")
    public double lambda; // GNN operator explosion coefficient. 1 means no explosion

    /**
     * {@link Dataset} to be used
     */
    protected Dataset dataset;

    /**
     * Should the resources be fineGrained, adding slotSharingGroups and etc.
     */
    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled; // Add custom slotSharingGroupsForOperators

    /**
     * Name of the partitioner to be resolved to {@code this.partitionerInstance}
     * <strong> You can leave it blank and populate {@code this.partitionerInstance} manually </strong>
     */
    @CommandLine.Option(names = {"-p", "--partitioner"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Partitioner to be used")
    protected String partitionerName;

    /**
     * Name of the dataset to be resolved to {@code this.datasetInstance}
     * <strong> You can leave it blank and populate {@code this.datasetInstance} manually </strong>
     */
    @CommandLine.Option(names = {"-d", "--dataset"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Dataset to be used")
    protected String datasetName;

    /**
     * Internal variable for creating Storage layers
     */
    protected short internalPositionIndex; // Counter of the Current GNN layer being generated

    /**
     * Internal variable for storing last iteration ID
     */
    protected IterationID internalLastIterationID; // Previous Layer Iteration Id used for backward message sending

    /**
     * Internal variable for storing first iteration id
     */
    protected IterationID internalFullLoopIterationId; // Iteration Id of 0 layer

    @SafeVarargs
    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, KeyedProcessFunction<PartNumber, GraphOp, GraphOp>... processFunctions) {
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.env.getConfig().enableObjectReuse();
        this.configureSerializers();
        this.hasFullLoopIteration = hasFullLoopIteration;
        this.hasBackwardIteration = hasBackwardIteration;
        this.hasLastLayerTopology = hasLastLayerTopology;
        this.processFunctions = processFunctions;
        this.layers = (short) processFunctions.length;
        this.dataset = Dataset.getDataset(datasetName, cmdArgs);
        this.partitioner = Partitioner.getPartitioner(partitionerName, cmdArgs);
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
        env.registerType(DirectedEdge.class);
        env.registerType(Feature.class);
        env.registerType(Parts.class);
        env.registerType(Tensor.class);
        env.registerType(Rmi.class);
        env.registerType(MeanAggregator.class);
        env.registerType(PartNumber.class);
    }

    /**
     * Helper method for creating single Storage Layer
     */
    protected SingleOutputStreamOperator<GraphOp> streamingStorageLayer(DataStream<GraphOp> inputData, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFunction) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(internalPositionIndex - 1, 0)));
        IterationID localIterationId = new IterationID();
        SingleOutputStreamOperator<GraphOp> forward;
        if (internalPositionIndex > 0 || hasFullLoopIteration) {
            // Add the iteration heads
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", internalPositionIndex), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, internalPositionIndex, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", internalPositionIndex));
            forward.getTransformation().setCoLocationGroupKey("gnn-" + internalPositionIndex);
            if (fineGrainedResourceManagementEnabled) {
                forward.slotSharingGroup("gnn-" + Math.max(internalPositionIndex - 1, 0)); // position 0 and 1 in same slot sharing group preferably
            }
            if (internalPositionIndex == 0) {
                // This was Splitter Full-Loop iteration
                internalFullLoopIterationId = localIterationId;
            }
        } else {
            forward = inputData.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", internalPositionIndex), TypeInformation.of(GraphOp.class), new WrapperOperatorFactory(new KeyedProcessOperator(processFunction), localIterationId, internalPositionIndex, layers)).setParallelism(thisParallelism).uid(String.format("GNN Operator - %s", internalPositionIndex));
            if (fineGrainedResourceManagementEnabled)
                forward.slotSharingGroup("gnn-" + Math.max(internalPositionIndex - 1, 0));
        }
        if (internalPositionIndex > 0) {
            // Iteration Tails
            SingleOutputStreamOperator<Void> iterationHandler = forward.getSideOutput(BaseWrapperOperator.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform(String.format("IterationTail - %s", internalPositionIndex), TypeInformation.of(Void.class), new IterationTailOperator(localIterationId)).setParallelism(thisParallelism).uid(String.format("IterationTail - %s", internalPositionIndex));
            iterationHandler.getTransformation().setCoLocationGroupKey("gnn-" + internalPositionIndex);
            if (fineGrainedResourceManagementEnabled)
                iterationHandler.slotSharingGroup("gnn-" + Math.max(internalPositionIndex - 1, 0));
        }
        if (internalPositionIndex > 1 && hasBackwardIteration) {
            // Add Backward Iteration
            int previousParallelism = (int) (env.getParallelism() * Math.pow(lambda, internalPositionIndex - 2));
            SingleOutputStreamOperator<Void> backwardIteration = forward.getSideOutput(BaseWrapperOperator.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()).transform(String.format("BackwardTail - %s", internalPositionIndex - 1), TypeInformation.of(Void.class), new IterationTailOperator(this.internalLastIterationID)).setParallelism(previousParallelism).uid(String.format("BackwardTail - %s", internalPositionIndex - 1));
            backwardIteration.getTransformation().setCoLocationGroupKey("gnn-" + (internalPositionIndex - 1));
            if (fineGrainedResourceManagementEnabled)
                backwardIteration.slotSharingGroup("gnn-" + Math.max(internalPositionIndex - 2, 0));
        }
        if (internalPositionIndex == layers && hasFullLoopIteration) {
            // Add Full Loop Iteration
            SingleOutputStreamOperator<Void> fullLoopIteration = forward.getSideOutput(BaseWrapperOperator.FULL_ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()).transform("FullLoopTail", TypeInformation.of(Void.class), new IterationTailOperator(this.internalFullLoopIterationId)).setParallelism(env.getParallelism()).uid("FullLoopTail");
            fullLoopIteration.getTransformation().setCoLocationGroupKey("gnn-0");
            if (fineGrainedResourceManagementEnabled) fullLoopIteration.slotSharingGroup("gnn-0");
        }
        this.internalPositionIndex++;
        this.internalLastIterationID = localIterationId;
        return forward;
    }

    /**
     * Build the execution pipeline
     *
     * @return [dataset stream, partitioner output, splitter output, ...storage layers]
     */
    protected DataStream<GraphOp>[] build() {
        Preconditions.checkNotNull(dataset);
        Preconditions.checkNotNull(partitioner);
        Preconditions.checkState(internalPositionIndex == 0);
        SingleOutputStreamOperator<GraphOp>[] layerOutputs = new SingleOutputStreamOperator[layers + 3]; // the final return value
        layerOutputs[0] = (SingleOutputStreamOperator<GraphOp>) dataset.build(env);
        layerOutputs[1] = partitioner.setPartitions((short) env.getMaxParallelism()).partition(layerOutputs[0]);
        layerOutputs[2] = streamingStorageLayer(layerOutputs[1], dataset.getSplitter());

        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);

        for (int i = 0; i < layers; i++) {
            KeyedProcessFunction<PartNumber, GraphOp, GraphOp> processFn = processFunctions[i];
            if (i == 0) {
                layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2], processFn);
            } else if (i == layers - 1) {
                if (hasLastLayerTopology)
                    layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(topologyUpdates, trainTestSplit), processFn);
                else layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(trainTestSplit), processFn);
            } else {
                layerOutputs[i + 3] = streamingStorageLayer(layerOutputs[i + 2].union(topologyUpdates), processFn);
            }
        }
        return layerOutputs;
    }

}
