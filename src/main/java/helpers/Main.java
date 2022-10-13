package helpers;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtModel;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.embedding_layer.GNNEmbeddingTrainingPlugin;
import plugins.vertex_classification.VertexClassificationTrainingPlugin;
import storage.FlatObjectStorage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class Main {

    public static ArrayList<Model> layeredModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(128, true));
        sb.add(new SAGEConv(64, true));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(41).optBias(true).build())

        );
        PtModel model = (PtModel) Model.newInstance("GNN");
        model.setBlock(sb);
        model.getBlock().initialize(LifeCycleNDManager.getInstance(), DataType.FLOAT32, new Shape(602));
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

        ArrayList<Model> models = layeredModel(); // Get the model to be served
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataFlow
        GraphStream gs = new GraphStream(env, args);
        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(true, true, false,
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer(models.get(0)))
                        .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(0).getName()))
                ),
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer(models.get(1)))
                        .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(1).getName()))
                ),
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer(models.get(2)))
                        .withPlugin(new VertexClassificationTrainingPlugin(models.get(2).getName(), new SerializableLoss(new SoftmaxCrossEntropyLoss("crossEntropy", 1, -1, true, true))))
                )

        );
        String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
        String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), "Training");
        env.execute(jobName);
    }
}
