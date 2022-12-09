package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import elements.features.MeanAggregator;
import elements.features.Parts;
import elements.features.Tensor;
import functions.helpers.Limiter;
import functions.selectors.PartKeySelector;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import org.apache.flink.util.Preconditions;
import partitioner.Partitioner;
import picocli.CommandLine;
import storage.BaseStorage;

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
    protected final Tuple2<BaseStorage, List<Plugin>>[] processStorageAndPlugins;

    /**
     * List of {@link IterateStream} for processFunctions + the splitter
     */
    protected IterateStream<GraphOp, GraphOp>[] iterateStreams;

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
    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, boolean hasLastLayerTopology, boolean hasBackwardIteration, boolean hasFullLoopIteration, Tuple2<BaseStorage, List<Plugin>>... processStorageAndPlugins) {
        Preconditions.checkNotNull(env);
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.env.getConfig().enableObjectReuse();
        this.configureSerializers();
        this.hasFullLoopIteration = hasFullLoopIteration;
        this.hasBackwardIteration = hasBackwardIteration;
        this.hasLastLayerTopology = hasLastLayerTopology;
        this.processStorageAndPlugins = processStorageAndPlugins;
        this.layers = (short) processStorageAndPlugins.length;
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
    public GraphStream setDataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    /**
     * Manually set the {@link Partitioner}
     */
    public GraphStream setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    protected SingleOutputStreamOperator<GraphOp> addStorageOperator(DataStream<GraphOp> inputStream, Tuple2<BaseStorage, List<Plugin>> storageAndPlugins, short index){
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, index - 1));
        SingleOutputStreamOperator<GraphOp> storageOperator = inputStream.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", index), TypeExtractor.createTypeInfo(GraphOp.class), new GraphStorageOperatorFactory(storageAndPlugins.f1, storageAndPlugins.f0,index)).setParallelism(thisParallelism);
        iterateStreams[index] = IterateStream.startIteration(storageOperator);
        iterateStreams[index].closeIteration(storageOperator.getSideOutput(OutputTags.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector())); // Add self loop
        if(index > 1 && hasBackwardIteration) iterateStreams[index - 1].closeIteration(storageOperator.getSideOutput(OutputTags.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()));
        return storageOperator;
    }

    protected SingleOutputStreamOperator<GraphOp> addSplitterOperator(DataStream<GraphOp> inputStream, KeyedProcessFunction<PartNumber, GraphOp, GraphOp> splitter){
        int thisParallelism = env.getParallelism();
        SingleOutputStreamOperator<GraphOp> splitterOperator = inputStream.keyBy(new PartKeySelector()).process(splitter).setParallelism(thisParallelism).name("Splitter");
        iterateStreams[0] = IterateStream.startIteration(splitterOperator);
        return splitterOperator;
    }

    /**
     * Build the execution pipeline
     *
     * @return [dataset stream, partitioner output, splitter output, ...storage layers]
     */
    public DataStream<GraphOp>[] build() {
        Preconditions.checkNotNull(dataset);
        Preconditions.checkNotNull(partitioner);
        SingleOutputStreamOperator<GraphOp>[] layerOutputs = new SingleOutputStreamOperator[layers + 3]; // the final return value
        layerOutputs[0] = datasetLimit > 0 ? dataset.build(env).filter(new Limiter<>(datasetLimit)).setParallelism(1).name(String.format("Limiter[%s]", datasetLimit)) : (SingleOutputStreamOperator<GraphOp>) dataset.build(env);
        layerOutputs[1] = partitioner.setPartitions((short) env.getMaxParallelism()).partition(layerOutputs[0]);
        layerOutputs[2] = addSplitterOperator(layerOutputs[1], dataset.getSplitter());
//        layerOutputs[2] = layerOutputs[1];
        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(Dataset.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(Dataset.TRAIN_TEST_SPLIT_OUTPUT);

        for (short i = 1; i <= layers; i++) {
            Tuple2<BaseStorage, List<Plugin>> processFn = processStorageAndPlugins[i-1];
            if (i == 1) {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1], processFn, i);
            } else if (i == layers) {
                if (hasLastLayerTopology)
                    layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates, trainTestSplit), processFn, i);
                else layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(trainTestSplit), processFn, i);
            } else {
                layerOutputs[i + 2] = addStorageOperator(layerOutputs[i + 1].union(topologyUpdates), processFn, i);
            }
        }
        return layerOutputs;
    }

}
