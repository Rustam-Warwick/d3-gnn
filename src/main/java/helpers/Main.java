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
import ai.djl.nn.gnn.HyperSAGEConv;
import elements.GraphOp;
import functions.storage.StreamingStorageProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.hgnn_embedding.StreamingHGNNEmbeddingLayer;
import storage.FlatObjectStorage;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * Application entrypoint
 */
public class Main {
    // -d=tags-ask-ubuntu --tagsAskUbuntu:type=star-graph -p=hdrf --hdrf:lambda=1 -l=3 -f=true
    public static ArrayList<Model> layeredModel() {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new HyperSAGEConv(64, true));
        sb.add(new HyperSAGEConv(32, true));
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
        try {
            BaseNDManager.getManager().delay();
            ArrayList<Model> models = layeredModel(); // Get the model to be served
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, false, false,
                    new StreamingStorageProcessFunction(new FlatObjectStorage()
                            .withPlugin(new ModelServer<>(models.get(0)))
                            .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(0).getName(), true))
                    ),
                    new StreamingStorageProcessFunction(new FlatObjectStorage()
                            .withPlugin(new ModelServer<>(models.get(1)))
                            .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(1).getName(), true))
                    )


            ).build();

            String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
            String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), "SessionW-50ms");
            env.execute(jobName);
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
