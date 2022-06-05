package helpers;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.pytorch.engine.PtModel;
import ai.djl.training.ParameterStore;
import ai.djl.training.loss.CrossEntropyLoss;
import datasets.CoraFull;
import datasets.Dataset;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import partitioner.HDRF;
import plugins.debugging.PrintVertexPlugin;
import plugins.embedding_layer.MixedGNNEmbeddingLayer;
import plugins.embedding_layer.MixedGNNEmbeddingLayerTraining;
import plugins.embedding_layer.StreamingGNNEmbeddingLayer;
import plugins.vertex_classification.VertexLossReporter;
import plugins.vertex_classification.VertexOutputLayer;
import plugins.vertex_classification.VertexTrainingLayer;
import storage.TupleStorage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
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
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(8710));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            PtModel tmp = (PtModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });

        return models;
    }

    public static void main(String[] args) throws Exception {
        ArrayList<Model> models = layeredModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(15000); // Make high to not have watermarks
        env.setParallelism(2);
        env.setMaxParallelism(10);

        GraphStream gs = new GraphStream(env); // Number of GNN Layers
        Dataset dataset = new CoraFull(Path.of(System.getenv("DATASET_DIR"),"cora"));
        DataStream<GraphOp>[] datasetStreamList = dataset.build(env);
        DataStream<GraphOp> partitioned = gs.partition(datasetStreamList[0], new HDRF());
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(partitioned, List.of(
                dataset.trainTestSplitter(),
                new StreamingGNNLayerFunction(new TupleStorage()
                        .withPlugin(new ParameterStore(models.get(0)))
                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(0).getName(), true))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(0).getName()))
                ),
                new StreamingGNNLayerFunction(new TupleStorage()
                        .withPlugin(new ParameterStore(models.get(1)))
                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(1).getName(), true))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(1).getName()))
                )
        ));

        embeddings.process(new ProcessFunction<GraphOp, Void>() {
            public transient File outputFile;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String homePath = System.getenv("HOME");
                outputFile = new File(String.format("%s/metrics/%s/output-%s.csv",homePath,getRuntimeContext().getJobId(), getRuntimeContext().getIndexOfThisSubtask()));
                File parent = outputFile.getParentFile();
                try {
                    parent.mkdirs();
                    outputFile.createNewFile();
                } catch (IOException | IllegalStateException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void processElement(GraphOp value, ProcessFunction<GraphOp, Void>.Context ctx, Collector<Void> out) throws Exception {
                Files.write(outputFile.toPath(), String.format("%s,%s\n", ctx.timestamp(), ctx.timerService().currentProcessingTime()).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            }
        });


        env.execute("gnn");
//        Thread.sleep(20000);
//        System.out.println("Triggered savepoint");
//        c.triggerSavepoint("file:///Users/rustamwarwick/Documents/Projects/Flink-Partitioning/checkpoints", SavepointFormatType.NATIVE);
    }
}
