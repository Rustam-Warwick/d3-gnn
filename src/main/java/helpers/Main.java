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
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import plugins.ModelServer;
import plugins.gnn_embedding.AdaptiveWindowedGNNEmbedding;
import plugins.vertex_classification.BatchSizeBinaryVertexClassificationTraining;

import java.util.ArrayList;
import java.util.Date;
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
                        .add(Linear.builder().setUnits(1).optBias(true).build())
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
            ArrayList<Model> models = layeredModel(); // Get the model to be served
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            GraphStream gs = new GraphStream(env, args, (short) 2, (position, layers, extra) -> {
                if (position == 0)
                    return new DatasetSplitterOperatorFactory(layers, (KeyedProcessFunction<PartNumber, GraphOp, GraphOp>) extra[0]);
                if (position == 1)
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(0)),  new AdaptiveWindowedGNNEmbedding(models.get(0).getName(), true, 20, 2000)), position, layers);
                if (position == 2)
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(1)),  new AdaptiveWindowedGNNEmbedding(models.get(1).getName(), false, 20, 2000)), position, layers);
                else
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(2)), new BatchSizeBinaryVertexClassificationTraining(models.get(2).getName(), Loss.sigmoidBinaryCrossEntropyLoss(), 50000)), position, layers);
            });
            env.setMaxParallelism(270);
            gs.build();
            env.execute(String.format("Adaptive 20-1000 %s (%s) [%s]", new Date(), env.getParallelism(), String.join(",", args)));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
