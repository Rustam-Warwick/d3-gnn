package helpers;

import ai.djl.ndarray.NDHelper;
import datasets.Dataset;
import elements.*;
import elements.features.MeanAggregator;
import elements.features.Parts;
import elements.features.Tensor;
import functions.helpers.Throttler;
import functions.selectors.PartKeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.tmshared.TMSharedStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;
import partitioner.Partitioner;
import picocli.CommandLine;

import java.util.Arrays;

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
     * Position to {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator}
     * either {@link org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory}
     * or {@link org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperatorFactory}
     */
    protected final TriFunction<Short, Short, Object[], OneInputStreamOperatorFactory<GraphOp, GraphOp>> operatorFactorySupplier;

    /**
     * Number of GNN layers in the pipeline {@code processFunctions.length}
     */
    protected final short layers;

    /**
     * List of {@link IterateStream} for processFunctions + the splitter
     */
    protected final IterateStream<GraphOp, GraphOp>[] iterateStreams;

    /**
     * {@link Partitioner} to be used
     */
    protected Partitioner partitioner;

    /**
     * {@link Dataset} to be used
     */
    protected Dataset dataset;

    /**
     * Explosion coefficient across the Storage layers
     */
    @CommandLine.Option(names = {"-l", "--lambda"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = "explosion coefficient")
    protected double lambda; // GNN operator explosion coefficient. 1 means no explosion

    /**
     * Should the resources be fineGrained, adding slotSharingGroups etc.
     */
    @CommandLine.Option(names = {"-f", "--fineGrainedResourceManagementEnabled"}, defaultValue = "false", fallbackValue = "false", arity = "1", description = "Is fine grained resource management enabled")
    protected boolean fineGrainedResourceManagementEnabled;

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


    public GraphStream(StreamExecutionEnvironment env, String[] cmdArgs, short layers, TriFunction<Short, Short, Object[], OneInputStreamOperatorFactory<GraphOp, GraphOp>> operatorFactorySupplier) {
        Preconditions.checkNotNull(env);
        Preconditions.checkState(layers >= 1);
        Arrays.sort(cmdArgs);
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
        this.env = env;
        this.env.getConfig().enableObjectReuse();
        this.env.setStateBackend(TMSharedStateBackend.with(new HashMapStateBackend()));
        this.configureSerializers();
        this.layers = layers;
        this.operatorFactorySupplier = operatorFactorySupplier;
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
     *
     * @param inputStream incoming GraphOp for this layer, not partitioned yet
     * @param position    position of the storage layer [1...layers]
     * @return Output of this storage operator not partitioned
     */
    protected SingleOutputStreamOperator<GraphOp> addGraphOperator(DataStream<GraphOp> inputStream, short position, Object[] extra) {
        int thisParallelism = (int) (env.getParallelism() * Math.pow(lambda, Math.max(position - 1, 0)));
        SingleOutputStreamOperator<GraphOp> storageOperator = inputStream.keyBy(new PartKeySelector()).transform(String.format("GNN Operator - %s", position), TypeExtractor.createTypeInfo(GraphOp.class), operatorFactorySupplier.apply(position, layers, extra)).setParallelism(thisParallelism);
        if (fineGrainedResourceManagementEnabled && position > 1) storageOperator.slotSharingGroup("GNN-" + position);
        iterateStreams[position] = IterateStream.startIteration(storageOperator);
        return storageOperator;
    }

    /**
     * Second part of build responsible for storage operators, this part is modifiable(extensible)
     *
     * @param layerOutputs [dataset, partitioner, splitter, ... empty]
     */
    public DataStream<GraphOp>[] build(SingleOutputStreamOperator<GraphOp>[] layerOutputs) {
        DataStream<GraphOp> topologyUpdates = layerOutputs[2].getSideOutput(OutputTags.TOPOLOGY_ONLY_DATA_OUTPUT);
        DataStream<GraphOp> trainTestSplit = layerOutputs[2].getSideOutput(OutputTags.TRAIN_TEST_SPLIT_OUTPUT);
        for (short i = 1; i <= layers; i++) {
            if (i == 1) {
                layerOutputs[i + 2] = addGraphOperator(layerOutputs[2], i, null); // First directly from splitter
            } else if (i == layers) {
                layerOutputs[i + 2] = addGraphOperator(layerOutputs[i + 1].union(topologyUpdates), i, null); // Last without topology
            } else {
                layerOutputs[i + 2] = addGraphOperator(layerOutputs[i + 1].union(topologyUpdates), i, null); // Mid-topology + previous
            }
            iterateStreams[i].closeIteration(layerOutputs[i + 2].getSideOutput(OutputTags.ITERATE_OUTPUT_TAG).keyBy(new PartKeySelector()));
            iterateStreams[i - 1].closeIteration(layerOutputs[i + 2].getSideOutput(OutputTags.BACKWARD_OUTPUT_TAG).keyBy(new PartKeySelector()));
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
        layerOutputs[2] = addGraphOperator(layerOutputs[1].process(new Throttler(10000, 100000)).setParallelism(1), (short) 0, new Object[]{dataset.getSplitter()});
        return build(layerOutputs);
    }

}
