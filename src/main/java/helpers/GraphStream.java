package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import elements.features.MeanAggregator;
import elements.features.Parts;
import elements.features.Tensor;
import functions.helpers.Limiter;
import functions.selectors.PartKeySelector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperator;
import org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Preconditions;
import partitioner.Partitioner;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.List;

/**
 * Helper class for creating a pipeline
 * <p>
 * Pipeline usually starts with:
 * {@link Dataset} --> {@link Partitioner} --> Splitter --> Storage-1 --> ... --> Storage-[Layers]
 * </p>
 */
public class GraphStream {

    /**
     * Execution environment
     */
    protected final StreamExecutionEnvironment env;

    /**
     * List of storage and plugins where each one corresponds to single layer in GNN pipeline
     */
    protected final List<Plugin>[] plugins;
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
     * Number of Storage layers in the pipeline {@code processFunctions.length}
     */
    protected final short layers;
    /**
     * List of {@link IterateStream} for processFunctions + the splitter
     */
    protected IterateStream<GraphOp, GraphOp>[] iterateStreams;
    /**
     * {@link Partitioner} to be used
     */
    protected Partitioner partitioner;

    /**
     * Explosion coefficient across the Storage layers
     */
    @CommandLine.Option(names = {"-l", "--lambda"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = "explosion coefficient")
    protected double lambda; // GNN operator explosion coefficient. 1 means no explosion

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
     * Limit the number of elements streaming through the {@link Dataset}
     */
    @CommandLine.Option(names = {"--datasetLimit"}, defaultValue = "0", fallbackValue = "0", arity = "1", description = "Should the dataset be capped at some number of streamed elements")
    protected long datasetLimit;

    /**
     * Name of the dataset to be resolved to {@code this.datasetInstance}
     * <strong> You can leave it blank and populate {@code this.datasetInstance} manually </strong>
     */
    @CommandLine.Option(names = {"-d", "--dataset"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Dataset to be used")
    protected String datasetName;

    @SafeVarargs
    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, List<Plugin>... plugins) {
        Preconditions.checkNotNull(env);
        env.setStateBackend(TaskSharedStateBackend.with(new HashMapStateBackend()));
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.env.getConfig().enableObjectReuse();
        this.configureSerializers();
        this.hasFullLoopIteration = hasFullLoopIteration;
        this.hasBackwardIteration = hasBackwardIteration;
        this.hasLastLayerTopology = hasLastLayerTopology;
        this.plugins = plugins;
        this.layers = (short) plugins.length;
        this.iterateStreams = new IterateStream[this.layers + 1];
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
     * Manually set the {@link Dataset}
     */
    public final GraphStream setDataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    /**
     * Manually set the {@link Partitioner}
     */
    public final GraphStream setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Add Storage operator
     * @param inputStream incoming GraphOp for this layer, not partitioned yet
     * @param plugins List of Plugins to be used
     * @param index index of the storage layer [1...layers]
     * @return Output of this storage operator not partitioned
     */
    protected final SingleOutputStreamOperator<GraphOp> addStorageOperator(DataStream<GraphOp> inputStream, List<Plugin> plugins, short index) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, index - 1));
        SingleOutputStreamOperator<GraphOp> storageOperator = inputStream.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", index), TypeExtractor.createTypeInfo(GraphOp.class), new GraphStorageOperatorFactory(plugins, index)).setParallelism(thisParallelism);
        if (fineGrainedResourceManagementEnabled) storageOperator.slotSharingGroup("GNN-" + index);
        iterateStreams[index] = IterateStream.startIteration(storageOperator);
        iterateStreams[index].closeIteration(storageOperator.getSideOutput(OutputTags.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector())); // Add self loop
        if (index > 1 && hasBackwardIteration)
            iterateStreams[index - 1].closeIteration(storageOperator.getSideOutput(OutputTags.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()));
        return storageOperator;
    }

    /**
     * Add Splitter Operator
     * @param inputStream Input stream after partitioning this graph
     * @param splitter Splitter function usually getting from the Dataset object
     * @return Return the splitted dataset result with side output tags
     */
    protected final SingleOutputStreamOperator<GraphOp> addSplitterOperator(DataStream<GraphOp> inputStream, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> splitter) {
        int thisParallelism = env.getParallelism();
        SingleOutputStreamOperator<GraphOp> splitterOperator = inputStream.keyBy(new PartKeySelector()).transform("Splitter", TypeInformation.of(GraphOp.class), new DatasetSplitterOperatorFactory(splitter)).setParallelism(thisParallelism).name("Splitter");
        if (fineGrainedResourceManagementEnabled) splitterOperator.slotSharingGroup("GNN-1");
        iterateStreams[0] = IterateStream.startIteration(splitterOperator);
        return splitterOperator;
    }


    /**
     * Second part of build responsible for storage operators, this part is modifiable(extensible)
     * @param layerOutputs [datsset, partitioner, splitter, ... empty]
     */
    public DataStream<GraphOp>[] build(SingleOutputStreamOperator<GraphOp>[] layerOutputs){
        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(OutputTags.TRAIN_TEST_SPLIT_OUTPUT);

        for (short i = 1; i <= layers; i++) {
            List<Plugin> processFn = plugins[i - 1];
            if (i == 1) {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1], processFn, i); // First directly from splitter
            } else if (i == layers) {
                if (hasLastLayerTopology)
                    layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates, trainTestSplit), processFn, i); // last with topology
                else layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(trainTestSplit), processFn, i); // Last without topology
            } else {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates), processFn, i); // Mid topology + previour
            }
        }
        return layerOutputs;
    }

    /**
     * Build the execution pipeline
     *
     * @return [dataset stream, partitioner output, splitter output, ...storage layers]
     */
    public final DataStream<GraphOp>[] build() {
        Preconditions.checkNotNull(dataset);
        Preconditions.checkNotNull(partitioner);
        SingleOutputStreamOperator<GraphOp>[] layerOutputs = new SingleOutputStreamOperator[layers + 3]; // the final return value
        layerOutputs[0] = datasetLimit > 0 ? dataset.build(env).filter(new Limiter<>(datasetLimit)).setParallelism(1).name(String.format("Limiter[%s]", datasetLimit)) : (SingleOutputStreamOperator<GraphOp>) dataset.build(env);
        layerOutputs[1] = partitioner.setPartitions((short) env.getMaxParallelism()).partition(layerOutputs[0]);
        layerOutputs[2] = addSplitterOperator(layerOutputs[1], dataset.getSplitter());
        return build(layerOutputs);
    }

}
