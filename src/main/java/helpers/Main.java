package helpers;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.pytorch.engine.PtModel;
import datasets.Dataset;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.embedding_layer.WindowedGNNEmbeddingLayer;
import storage.FlatInMemoryClassStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class Main {
    public static ArrayList<Model> layeredModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(64, true));
        sb.add(new SAGEConv(32, true));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(64).optBias(true).build())
                        .add(Linear.builder().setUnits(70).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.softmax(ndArrays);
                            }
                        })
        );
        PtModel model = (PtModel) Model.newInstance("GNN");
        model.setBlock(sb);
//        model.load(Path.of("/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/jupyter/models/GraphSageBias-2022-05-15"));
//        model.load(Path.of("/dcs/pg21/u2154598/GraphSageBias-2022-05-15"));
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(64));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            PtModel tmp = (PtModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });

        return models;
    }

    public static void main(String[] args) throws Exception {
        // Configuration
        Arrays.sort(args);
        ArrayList<Model> models = layeredModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(30000);
        // Initializate the helper classes
        GraphStream gs = new GraphStream(env, args);
        // DataFlow
        Dataset dataset = Dataset.getDataset(gs.dataset);
        DataStream<GraphOp>[] datasetStreamList = dataset.build(env, gs.fineGrainedResourceManagementEnabled);
        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(datasetStreamList[0], true, false, false,
                dataset.trainTestSplitter(),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(0)))
                        .withPlugin(new WindowedGNNEmbeddingLayer(models.get(0).getName(), false, 10000))
//                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(0).getName(), false))
                ),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(1)))
                        .withPlugin(new WindowedGNNEmbeddingLayer(models.get(1).getName(), true, 20000))
//                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(1).getName(), true))
                )
        );

        String jobName = String.format("%s W-10000", String.join(" ", args), env.getMaxParallelism());
//        embeddings[embeddings.length - 1].process(new ProcessFunction<GraphOp, Object>() {
//            @Override
//            public void processElement(GraphOp value, ProcessFunction<GraphOp, Object>.Context ctx, Collector<Object> out) throws Exception {
//                System.out.println(ctx.timestamp());
//            }
//        });
        // Latency Calculations
//        embeddings[0]
//                .process(new AddTimestamp()).setParallelism(2).name("Inputs").keyBy(GraphOp::getTimestamp)
//                .connect(embeddings[embeddings.length - 1].process(new AddTimestamp()).setParallelism(embeddings[embeddings.length - 1].getParallelism() - 1).name("Embeddings").keyBy(GraphOp::getTimestamp))
//                .process(new LatencyOutput(jobName, 10000)).setParallelism(embeddings[embeddings.length - 1].getParallelism());

        env.execute(jobName);
        System.gc();

//        Thread.sleep(20000);
//        System.out.println("Triggered savepoint");
//        c.triggerSavepoint("file:///Users/rustamwarwick/Documents/Projects/Flink-Partitioning/checkpoints", SavepointFormatType.NATIVE);
    }
}
