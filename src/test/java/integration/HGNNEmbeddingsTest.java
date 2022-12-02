package integration;

import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.hgnn.HGNNBlock;
import ai.djl.training.ParameterStore;
import elements.GraphOp;
import elements.features.Tensor;
import functions.storage.StreamingStorageProcessFunction;
import helpers.GraphStream;
import helpers.datasets.MeshHyperGraphGenerator;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import plugins.ModelServer;
import plugins.debugging.LogCallbacksPlugin;
import plugins.hgnn_embedding.SessionWindowedHGNNEmbeddingLayer;
import plugins.hgnn_embedding.StreamingHGNNEmbeddingLayer;
import storage.FlatObjectStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class HGNNEmbeddingsTest extends IntegrationTest {
    private static final Map<String, NDArray> vertexEmbeddings = new HashMap<>();

    private static Stream<Arguments> jobArguments() {
        return Stream.of(
                Arguments.arguments(new String[]{"-p=hypergraph-minmax", "-l=2"}, 1, 10),
                Arguments.arguments(new String[]{"-p=hypergraph-minmax", "-l=2"}, 2, 10),
                Arguments.arguments(new String[]{"-p=random", "-l=2"}, 1, 10),
                Arguments.arguments(new String[]{"-p=random", "-l=2"}, 2, 10)
        );
    }

    @ParameterizedTest
    @MethodSource("jobArguments")
    @Disabled
    void testSessionWindowlugin(String[] args, int layers, int meshSize) throws Exception {
        try {
            BaseNDManager.getManager().delay();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            ArrayList<Model> models = getHGNNModel(layers); // Get the model to be served
            KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctions = new KeyedProcessFunction[layers];
            for (int i = 0; i < layers; i++) {
                processFunctions[i] = new StreamingStorageProcessFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(i)))
                        .withPlugin(new SessionWindowedHGNNEmbeddingLayer(models.get(i).getName(), true, 50))
                        .withPlugin(new LogCallbacksPlugin())
                );
            }
            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, false, false, processFunctions).setDataset(new MeshHyperGraphGenerator(meshSize)).build();
            gs[gs.length - 1].process(new CollectEmbeddingsProcess()).setParallelism(1);
            env.execute();
            verifyEmbeddings(meshSize, models);
        } finally {
            vertexEmbeddings.values().forEach(NDArray::resume);
            vertexEmbeddings.clear();
            BaseNDManager.getManager().resume();
        }
    }

    @ParameterizedTest
    @MethodSource("jobArguments")
    void testStreamingPlugin(String[] args, int layers, int meshSize) throws Exception {
        try {
            BaseNDManager.getManager().delay();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            ArrayList<Model> models = getHGNNModel(layers); // Get the model to be served
            KeyedProcessFunction<PartNumber, GraphOp, GraphOp>[] processFunctions = new KeyedProcessFunction[layers];
            for (int i = 0; i < layers; i++) {
                processFunctions[i] = new StreamingStorageProcessFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(i)))
                        .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(i).getName(), true))
                        .withPlugin(new LogCallbacksPlugin())
                );
            }
            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, false, false, processFunctions).setDataset(new MeshHyperGraphGenerator(meshSize)).build();
            gs[gs.length - 1].process(new CollectEmbeddingsProcess()).setParallelism(1);
            env.execute();
            verifyEmbeddings(meshSize, models);
        } finally {
            vertexEmbeddings.values().forEach(NDArray::resume);
            vertexEmbeddings.clear();
            BaseNDManager.getManager().resume();
        }
    }

    private void verifyEmbeddings(int meshSize, ArrayList<Model> models) {
        ParameterStore store = new ParameterStore();
        NDArray previousLayerEmbedding = BaseNDManager.getManager().ones(models.get(0).describeInput().get(0).getValue());
        for (Model model : models) {
            HGNNBlock block = (HGNNBlock) model.getBlock();
            NDArray message = block.message(store, new NDList(previousLayerEmbedding), false).get(0);
            NDArray aggregator = message.mul(meshSize * meshSize);
            previousLayerEmbedding = block.update(store, new NDList(previousLayerEmbedding, aggregator), false).get(0);
        }
        for (Map.Entry<String, NDArray> stringNDArrayEntry : vertexEmbeddings.entrySet()) {
            Assertions.assertTrue(stringNDArrayEntry.getValue().allClose(previousLayerEmbedding, 1e-4, 1e-04, false));
        }
    }


    private static class CollectEmbeddingsProcess extends ProcessFunction<GraphOp, Void> {
        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
            Tensor tensor = (Tensor) value.element;
            vertexEmbeddings.compute(tensor.ids.f1, (vertexId, oldTensor) -> {
                if (oldTensor != null) oldTensor.resume();
                tensor.delay();
                return tensor.getValue();
            });
        }
    }
}
