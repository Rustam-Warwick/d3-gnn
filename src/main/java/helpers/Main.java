package helpers;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.gnn_embedding.SessionWindowedGNNEmbeddingLayer;
import storage.FlatObjectStorage;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.function.Function;

public class Main {
    public static ArrayList<Model> layeredModel() {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(64, true));
        sb.add(new SAGEConv(32, true));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(41).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.relu(ndArrays);
                            }
                        })

        );
        BaseModel model = (BaseModel) Model.newInstance("GNN");
        model.setBlock(sb);
        model.getBlock().initialize(BaseNDManager.getManager(), DataType.FLOAT32, new Shape(128));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            BaseModel tmp = (BaseModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });
        return models;
    }

    public static void main(String[] args) throws Throwable {
        // Configuration
        BaseNDManager.getManager().delay();
        ArrayList<Model> models = layeredModel(); // Get the model to be served
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataFlow
        GraphStream gs = new GraphStream(env, args);
        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(true, false, false,
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(0)))
                        .withPlugin(new SessionWindowedGNNEmbeddingLayer(models.get(0).getName(), true, 50))
//                       .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(0).getName(), false))
                ),
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(1)))
                        .withPlugin(new SessionWindowedGNNEmbeddingLayer(models.get(0).getName(), true, 50))
//                       .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(0).getName(), false))
                )
        );
        String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
        String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), "Window-200ms");
        env.execute(jobName);
        BaseNDManager.getManager().resume();
    }
}
