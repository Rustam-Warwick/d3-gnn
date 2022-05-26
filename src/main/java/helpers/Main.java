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
import ai.djl.training.ParameterStore;
import datasets.CoraFull;
import datasets.Dataset;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.HDRF;
import plugins.debugging.PrintVertexPlugin;
import plugins.embedding_layer.MixedGNNEmbeddingLayer;
import plugins.vertex_classification.VertexLossReporter;
import plugins.vertex_classification.VertexOutputLayer;
import storage.TupleStorage;

import java.io.IOException;
import java.nio.file.Path;
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
        model.load(Path.of("/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/jupyter/models/GraphSageBias-2022-05-15"));
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
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getConfig().setAutoWatermarkInterval(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///home/rustambaku13/Documents/Warwick/flink-streaming-gnn/checkpoints");
        env.setParallelism(2);
        env.setMaxParallelism(30);

        GraphStream gs = new GraphStream(env); // Number of GNN Layers
        Dataset dataset = new CoraFull(Path.of("/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/jupyter/datasets/cora"));
        DataStream<GraphOp>[] datasetStreamList = dataset.build(env);
        DataStream<GraphOp> partitioned = gs.partition(datasetStreamList[0], new HDRF());
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(partitioned, List.of(
                dataset.trainTestSplitter(),
                new StreamingGNNLayerFunction(new TupleStorage()
                        .withPlugin(new ParameterStore(models.get(0)))
                        .withPlugin(new MixedGNNEmbeddingLayer(models.get(0).getName(), true))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(0).getName()))
                ),
                new StreamingGNNLayerFunction(new TupleStorage()
                        .withPlugin(new ParameterStore(models.get(1)))
                        .withPlugin(new MixedGNNEmbeddingLayer(models.get(1).getName(), true))
//                        .withPlugin(new MixedGNNEmbeddingLayerTraining(models.get(1).getName()))
                ),
                new StreamingGNNLayerFunction(new TupleStorage()
                        .withPlugin(new ParameterStore(models.get(2)))
                        .withPlugin(new VertexOutputLayer(models.get(2).getName()))
//                        .withPlugin(new VertexTrainingLayer(models.get(2).getName(), new SerializableLoss(new CrossEntropyLoss("loss", true))))
                        .withPlugin(new VertexLossReporter(models.get(2).getName()))
                        .withPlugin(new PrintVertexPlugin("193"))
                )
        ));

        env.execute("gnn");
    }
}
