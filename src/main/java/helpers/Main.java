package helpers;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.HyperSAGEConv;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import elements.GraphOp;
import elements.Vertex;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.tensorflow.ndarray.NdArrays;
import plugins.ModelServer;
import plugins.gnn_embedding.StreamingGNNEmbeddingLayer;
import plugins.hgnn_embedding.StreamingHGNNEmbeddingLayer;
import storage.FlatObjectStorage;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Main {
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

    public static void test(){
        NDManager manager = BaseNDManager.getManager();
        NDArray ten = manager.create(10);
        NDArray two = manager.create(2);
        ByteBuffer tenBuffer = ten.toByteBuffer();
        System.out.println(tenBuffer.array());
        ten.muli(two).muli(two);
        System.out.println(tenBuffer.array());
        Object a = NdArrays.ofBooleans(org.tensorflow.ndarray.Shape.of(1,1));
        System.out.println(a);
    }

    public static void main(String[] args) throws Throwable {
        // Configuration
        test();
        BaseNDManager.getManager().delay();
        ArrayList<Model> models = layeredModel(); // Get the model to be served
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataFlow
        GraphStream gs = new GraphStream(env, args);
        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(true, false, false,
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(0)))
                        .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(0).getName(), true))
//                       .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(0).getName(), false))
                ),
                new StreamingGNNLayerFunction(new FlatObjectStorage()
                        .withPlugin(new ModelServer<>(models.get(1)))
                        .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(1).getName(), true))
//                       .withPlugin(new GNNEmbeddingTrainingPlugin(models.get(0).getName(), false))
                )
        );

        String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
        String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), "Training");
        env.execute(jobName);
        BaseNDManager.getManager().resume();
    }
}
