package helpers;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.gnn.SAGEConv;
import ai.djl.training.loss.Loss;
import datasets.Dataset;
import datasets.RedditHyperlink;
import elements.DirectedEdge;
import elements.GraphOp;
import elements.Plugin;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.graph.DatasetSplitterOperatorFactory;
import org.apache.flink.streaming.api.operators.graph.GraphOperatorCoordinator;
import org.apache.flink.streaming.api.operators.graph.GraphStorageOperatorFactory;
import org.apache.flink.types.Row;
import partitioner.Partitioner;
import picocli.CommandLine;
import plugins.ModelServer;
import plugins.gnn_embedding.*;
import plugins.vertex_classification.BatchSizeBinaryVertexClassificationTraining;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            GraphStream gs = new GraphStream(env, args, (short) 2, (position, layers, graphStream) -> {
                if (position == 0)
                    return new DatasetSplitterOperatorFactory(layers, graphStream.dataset.getSplitter(), new GraphOperatorCoordinator.EmptyGraphOperatorSubCoordinatorsProvider());
                if (position == 1)
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(0)), graphStream.getModelPlugin(models.get(0).getName(), true)), position, layers, new GraphOperatorCoordinator.EmptyGraphOperatorSubCoordinatorsProvider());
                if (position == 2)
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(1)), graphStream.getModelPlugin(models.get(0).getName(), false)), position, layers, new GraphOperatorCoordinator.EmptyGraphOperatorSubCoordinatorsProvider());
                else
                    return new GraphStorageOperatorFactory(List.of(new ModelServer<>(models.get(2)), new BatchSizeBinaryVertexClassificationTraining(models.get(2).getName(), Loss.sigmoidBinaryCrossEntropyLoss(), 800)), position, layers);
            });
            env.setMaxParallelism(400);
            gs.build();
            env.execute(String.format("%s %s (%s) [%s]", gs.pluginName, new Date(), env.getParallelism(), String.join(",", args)));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
