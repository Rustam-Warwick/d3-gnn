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
import ai.djl.training.loss.Loss;
import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import plugins.ModelServer;
import plugins.gnn_embedding.SessionWindowedGNNEmbeddingLayer;
import plugins.scheduler.BatchSizeTrainingScheduler;
import plugins.vertex_classification.VertexClassificationTrainingPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Application entrypoint
 */
public class Main {
    // -d=tags-ask-ubuntu --tagsAskUbuntu:type=star-graph -p=hdrf --hdrf:lambda=1 -l=3 -f=true
    public static ArrayList<Model> layeredModel() {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(128, true));
        sb.add(new SAGEConv(64, true));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(32).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.relu(ndArrays);
                            }
                        })
                        .add(Linear.builder().setUnits(16).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.relu(ndArrays);
                            }
                        })
                        .add(Linear.builder().setUnits(2).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.sigmoid(ndArrays);
                            }
                        })

        );
        BaseModel model = (BaseModel) Model.newInstance("GNN");
        model.setBlock(sb);
//        NDHelper.loadModel(Path.of(System.getenv("DATASET_DIR"), "ogb-products", "graphSage"), model);
        model.getBlock().initialize(BaseNDManager.getManager(), DataType.FLOAT32, new Shape(17));
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
            DataStream<GraphOp>[] res = new GraphStream(env, args, false, false, false,
                    (pos, layers) -> new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(0)), new SessionWindowedGNNEmbeddingLayer(models.get(0).getName(), false, 100)), pos, layers),
                    (pos, layers) -> new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(1)), new SessionWindowedGNNEmbeddingLayer(models.get(1).getName(), false, 100)), pos, layers),
                    (pos, layers) -> new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(2)), new VertexClassificationTrainingPlugin(models.get(2).getName(), Loss.l1Loss()),
            new BatchSizeTrainingScheduler(256)), pos, layers)
            ).build();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
