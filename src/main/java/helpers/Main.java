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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import plugins.ModelServer;
import plugins.debugging.PrintVertexPlugin;
import plugins.embedding_layer.StreamingGNNEmbeddingLayer;
import storage.FlatInMemoryClassStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

public class Main {
    public static ArrayList<Model> layeredModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(128, true));
        sb.add(new SAGEConv(64, true));
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
//        model.load(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/models/GraphSageBias-2022-05-15"));
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(128));
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
        ArrayList<Model> models = layeredModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Begin the Dataflow Graph
        env.setParallelism(2);
        GraphStream gs = new GraphStream(env).parseCmdArgs(args); // Number of GNN Layers
//        env.setMaxParallelism((int) (env.getParallelism() * Math.pow(gs.lambda, 3) * 5));
        env.setMaxParallelism(5);

        Dataset dataset = Dataset.getDataset(gs.dataset);
        DataStream<GraphOp>[] datasetStreamList = dataset.build(env);
        DataStream<GraphOp> partitioned = gs.partition(datasetStreamList[0]);
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(partitioned,true,
                dataset.trainTestSplitter(),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(0)))
                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(0).getName(), false))
                        .withPlugin(new PrintVertexPlugin("1","2"))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(0).getName()))
                ),
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(1)))
                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(1).getName(), true))
                        .withPlugin(new PrintVertexPlugin("1","2"))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(1).getName()))
                )
        );


        String jobName = String.format("P-%s D-%s L-%s Par-%s MaxPar-%s",gs.partitionerName,gs.dataset,gs.lambda, env.getParallelism(), env.getMaxParallelism());
//        partitioned
//                .process(new AddTimestamp()).name("Inputs").keyBy(GraphOp::getTimestamp)
//                .connect(embeddings.process(new AddTimestamp()).name("Embeddings").keyBy(GraphOp::getTimestamp))
//                .process(new LatencyOutput(1000));
        embeddings.process(new ProcessFunction<GraphOp, GraphOp>() {
            @Override
            public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
                System.out.format("%s,%s\n", value, ctx.timestamp());
            }

        });
        env.execute(jobName);

//        Thread.sleep(20000);
//        System.out.println("Triggered savepoint");
//        c.triggerSavepoint("file:///Users/rustamwarwick/Documents/Projects/Flink-Partitioning/checkpoints", SavepointFormatType.NATIVE);
    }
}
