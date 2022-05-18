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
import ai.djl.training.loss.CrossEntropyLoss;
import datasets.CoraFull;
import datasets.Dataset;
import elements.GraphOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.HDRF;
import plugins.debugging.PrintVertexPlugin;
import plugins.embedding_layer.GNNEmbeddingLayer;
import plugins.embedding_layer.GNNPeriodicalEmbeddingLayer;
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
        model.load(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/models/GraphSageBias-2022-05-15"));
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(8710));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            PtModel tmp = (PtModel) Model.newInstance("GNN");
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });

        return models;
    }

    public static void main(String[] args) throws Exception {
        ArrayList<Model> models = layeredModel();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setMaxParallelism(10);


        // GraphStream
        GraphStream gs = new GraphStream(env, (short) 3); // Number of GNN Layers
        Dataset dataset = new CoraFull(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/datasets/cora"));

        DataStream<GraphOp>[] datasetStreamList = dataset.build(env);
        DataStream<GraphOp> partitioned = gs.partition(datasetStreamList[0], new HDRF());

        SingleOutputStreamOperator<GraphOp> trainTestSplit = partitioned.process(dataset.trainTestSplitter());
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(trainTestSplit, List.of(
                new TupleStorage()
                        .withPlugin(new GNNPeriodicalEmbeddingLayer(models.get(0), true))
                        .withPlugin(new PrintVertexPlugin())
                ,
                new TupleStorage()
                        .withPlugin(new GNNPeriodicalEmbeddingLayer(models.get(1), true))
                        .withPlugin(new PrintVertexPlugin())


        ));

        DataStream<GraphOp> trainFeatures = trainTestSplit.getSideOutput(Dataset.TRAIN_TEST_DATA_OUTPUT);
        SingleOutputStreamOperator<GraphOp> output = gs.gnnLayerNewIteration(
                embeddings.union(trainFeatures),
                new TupleStorage()
                        .withPlugin(new VertexLossReporter(new SerializableLoss(new CrossEntropyLoss("loss", true))))
                        .withPlugin(new VertexOutputLayer(models.get(2)))
                        );

        env.execute();
    }
}
