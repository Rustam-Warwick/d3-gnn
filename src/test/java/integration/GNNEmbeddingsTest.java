package integration;

import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.training.ParameterStore;
import elements.GraphOp;
import elements.features.Tensor;
import helpers.GraphStream;
import helpers.datasets.MeshGraphGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import plugins.ModelServer;
import plugins.debugging.LogCallbacks;
import plugins.gnn_embedding.SessionWindowedGNNEmbedding;
import plugins.gnn_embedding.StreamingGNNEmbedding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class GNNEmbeddingsTest extends IntegrationTest {
    private static final Map<Object, NDArray> vertexEmbeddings = new HashMap<>();

    private static Stream<Arguments> jobArguments() {
        return Stream.of(
                Arguments.arguments(new String[]{"-p=hdrf", "-l=1.3"}, 1, 10),
                Arguments.arguments(new String[]{"-p=hdrf", "-l=1.3"}, 2, 10),
                Arguments.arguments(new String[]{"-p=random", "-l=1.3"}, 1, 10),
                Arguments.arguments(new String[]{"-p=random", "-l=1.3"}, 2, 10)
        );
    }

    @ParameterizedTest
    @MethodSource("jobArguments")
    void testStreamingPlugin(String[] args, int layers, int meshSize) throws Exception {
        try {
            BaseNDManager.getManager().delay();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
            ArrayList<Model> models = getGNNModel(layers); // Get the model to be served
            BiFunction<Short, Short, GraphStorageOperatorFactory>[] processFunctions = new BiFunction[layers];
            for (int i = 0; i < layers; i++) {
                processFunctions[i] =
                        (pos, layer) -> new GraphStorageOperatorFactory(
                        List.of(
                                new ModelServer<>(models.get(pos-1)),
                                new StreamingGNNEmbedding(models.get(pos-1).getName(), true),
                                new LogCallbacks()
                        ), pos, layer);
            }
            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, processFunctions).setDataset(new MeshGraphGenerator(meshSize)).build();
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
    void testSessionWindowPlugin(String[] args, int layers, int meshSize) throws Exception {
        try {
            BaseNDManager.getManager().delay();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);
            ArrayList<Model> models = getGNNModel(layers); // Get the model to be served
            BiFunction<Short, Short, GraphStorageOperatorFactory>[] processFunctions = new BiFunction[layers];
            for (int i = 0; i < layers; i++) {
                processFunctions[i] =
                        (pos, layer) -> new GraphStorageOperatorFactory(
                                List.of(
                                        new ModelServer<>(models.get(pos-1)),
                                        new SessionWindowedGNNEmbedding(models.get(pos-1).getName(), true, 150),
                                        new LogCallbacks()
                                ), pos, layer);
            }
            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, processFunctions).setDataset(new MeshGraphGenerator(meshSize)).build();
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
            GNNBlock block = (GNNBlock) model.getBlock();
            NDArray message = block.message(store, new NDList(previousLayerEmbedding), false).get(0);
            NDArray aggregator = message.mul(meshSize - 1);
            previousLayerEmbedding = block.update(store, new NDList(previousLayerEmbedding, aggregator), false).get(0);
        }
        for (Map.Entry<Object, NDArray> stringNDArrayEntry : vertexEmbeddings.entrySet()) {
            Assertions.assertTrue(stringNDArrayEntry.getValue().allClose(previousLayerEmbedding, 1e-4, 1e-04, false));
        }
    }


    private static class CollectEmbeddingsProcess extends ProcessFunction<GraphOp, Void> {

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
            Tensor tensor = (Tensor) value.element;
            vertexEmbeddings.compute(tensor.id.f1, (vertexId, oldTensor) -> {
                if (oldTensor != null) oldTensor.resume();
                tensor.delay();
                return tensor.getValue();
            });
        }
    }
}
