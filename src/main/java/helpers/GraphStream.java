package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import elements.features.MeanAggregator;
import elements.features.Parts;
import elements.features.Tensor;
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
import org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Preconditions;
import partitioner.Partitioner;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.function.BiFunction;

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
    protected final BiFunction<Short,Short, GraphStorageOperatorFactory>[] operatorFactorySuppliers;
    /**
     * If the last storage layer should receive topology updates
     */
    protected final boolean hasLastLayerTopology;

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
     * Name of the dataset to be resolved to {@code this.datasetInstance}
     * <strong> You can leave it blank and populate {@code this.datasetInstance} manually </strong>
     */
    @CommandLine.Option(names = {"-d", "--dataset"}, defaultValue = "", fallbackValue = "", arity = "1", description = "Dataset to be used")
    protected String datasetName;

    @SafeVarargs
    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, boolean hasLastLayerTopology, BiFunction<Short, Short, GraphStorageOperatorFactory>... operatorFactorySuppliers) {
        Preconditions.checkNotNull(env);
        env.setStateBackend(TaskSharedStateBackend.with(new HashMapStateBackend()));
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.env.getConfig().enableObjectReuse();
        this.configureSerializers();
        this.hasLastLayerTopology = hasLastLayerTopology;
        this.operatorFactorySuppliers = operatorFactorySuppliers;
        this.layers = (short) operatorFactorySuppliers.length;
        this.iterateStreams = new IterateStream[this.layers + 1]; // SPLITTER + Storage operator
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
     *
     * @param inputStream incoming GraphOp for this layer, not partitioned yet
     * @param operatorFactorySupplier     List of Plugins to be used
     * @param position       position of the storage layer [1...layers]
     * @return Output of this storage operator not partitioned
     */
    protected final SingleOutputStreamOperator<GraphOp> addStorageOperator(DataStream<GraphOp> inputStream, BiFunction<Short, Short, GraphStorageOperatorFactory> operatorFactorySupplier, short position) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, position - 1));
        SingleOutputStreamOperator<GraphOp> storageOperator = inputStream.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position), TypeExtractor.createTypeInfo(GraphOp.class), operatorFactorySupplier.apply(position, layers)).setParallelism(thisParallelism);
        if (fineGrainedResourceManagementEnabled) storageOperator.slotSharingGroup("GNN-" + position);
        iterateStreams[position] = IterateStream.startIteration(storageOperator);
        iterateStreams[position].closeIteration(storageOperator.getSideOutput(OutputTags.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()));
        return storageOperator;
    }

    /**
     * Add Splitter Operator
     *
     * @param inputStream Input stream after partitioning this graph
     * @param splitter    Splitter function usually getting from the Dataset object
     * @return Return the splitter dataset result with side output tags
     */
    protected final SingleOutputStreamOperator<GraphOp> addSplitterOperator(DataStream<GraphOp> inputStream, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> splitter) {
        int thisParallelism = env.getParallelism();
        SingleOutputStreamOperator<GraphOp> splitterOperator = inputStream.keyBy(new PartKeySelector()).transform("Splitter", TypeInformation.of(GraphOp.class), new DatasetSplitterOperatorFactory(layers,splitter)).setParallelism(thisParallelism).name("Splitter");
        if (fineGrainedResourceManagementEnabled) splitterOperator.slotSharingGroup("GNN-1");
        iterateStreams[0] = IterateStream.startIteration(splitterOperator);
        return splitterOperator;
    }


    /**
     * Second part of build responsible for storage operators, this part is modifiable(extensible)
     *
     * @param layerOutputs [datsset, partitioner, splitter, ... empty]
     */
    public DataStream<GraphOp>[] build(SingleOutputStreamOperator<GraphOp>[] layerOutputs) {
        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(OutputTags.TRAIN_TEST_SPLIT_OUTPUT);

        for (short i = 1; i <= layers; i++) {
            BiFunction<Short,Short, GraphStorageOperatorFactory> operatorFactorySupplier = operatorFactorySuppliers[i - 1];
            if (i == 1) {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1], operatorFactorySupplier, i); // First directly from splitter
            } else if (i == layers) {
                if (hasLastLayerTopology)
                    layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates, trainTestSplit), operatorFactorySupplier, i); // last with topology
                else
                    layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(trainTestSplit), operatorFactorySupplier, i); // Last without topology
            } else {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates), operatorFactorySupplier, i); // Mid topology + previour
            }

            iterateStreams[i-1].closeIteration(layerOutputs[i+2].getSideOutput(OutputTags.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()));
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
        layerOutputs[0] = (SingleOutputStreamOperator<GraphOp>) dataset.build(env);
        layerOutputs[1] = partitioner.setPartitions((short) env.getMaxParallelism()).partition(layerOutputs[0]);
        layerOutputs[2] = addSplitterOperator(layerOutputs[1], dataset.getSplitter());
        return build(layerOutputs);
    }

}
