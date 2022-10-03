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
import elements.GraphOp;
import functions.gnn_layers.StreamingGNNLayerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import plugins.ModelServer;
import plugins.embedding_layer.StreamingGNNEmbeddingLayer;
import storage.FlatInMemoryClassStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

public class Main {

    public static ArrayList<Model> layeredModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(64, true));
        sb.add(new SAGEConv(32, true));
        sb.add(
                new SequentialBlock()
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                if (ndArrays.size() == 1) {
                                    return new NDList(ndArrays.get(0).concat(ndArrays.get(0), -1));
                                }
                                return new NDList(ndArrays.get(0).concat(ndArrays.get(1), -1));
                            }
                        })
                        .add(Linear.builder().setUnits(16).optBias(true).build())
                        .add(Linear.builder().setUnits(1).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.leakyRelu(ndArrays, 1);
                            }
                        })

        );
        PtModel model = (PtModel) Model.newInstance("GNN");
        model.setBlock(sb);
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(64));
        model.getBlock().getParameters().forEach(item -> item.getValue().getArray().postpone());
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
        env.setMaxParallelism(2);
        env.setParallelism(2);
        // DataFlow
//        Integer window = null;
        GraphStream gs = new GraphStream(env, args);

        DataStream<GraphOp>[] embeddings = gs.gnnEmbeddings(true, false, false,
                new StreamingGNNLayerFunction(new FlatInMemoryClassStorage()
                        .withPlugin(new ModelServer(models.get(0)))
                        .withPlugin(new StreamingGNNEmbeddingLayer(models.get(0).getName(), true))
                )
        );

//        String timeStamp = new SimpleDateFormat("MM.dd.HH.mm").format(new java.util.Date());
//        String jobName = String.format("%s (%s) [%s] %s", timeStamp, env.getParallelism(), String.join(" ", args), window == null ? "Streaming" : "Window-" + window);
        env.execute();


    }
}
