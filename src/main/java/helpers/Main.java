package helpers;

import ai.djl.MalformedModelException;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.training.loss.Loss;
import datasets.CoraFull;
import datasets.Dataset;
import elements.GraphOp;
import functions.nn.LossWrapper;
import functions.nn.MyActivations;
import functions.nn.SerializableModel;
import functions.nn.gnn.SAGEConv;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import partitioner.HDRF;
import plugins.newblock.embedding_layer.GNNStreamingEmbeddingLayer;
import plugins.vertex_classification.VertexClassificationTester;
import plugins.vertex_classification.VertexClassificationLayer;
import storage.TupleStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

public class Main {
    public static SerializableModel<SequentialBlock> myPartitionedModel() throws MalformedModelException, IOException {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new SAGEConv(128));
        sb.add(new SAGEConv(128));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(64).optBias(false).build())
                        .add(Linear.builder().setUnits(70).optBias(false).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return MyActivations.softmax(ndArrays);
                            }
                        })

        );
        SerializableModel<SequentialBlock> model = new SerializableModel<>("gnn", sb);
        model.setManager(NDManager.newBaseManager());

        model.load(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/models/GraphSage-CoraFull-2022-05-08"));
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(8710));
        return model;
    }

    public static void main(String[] args) throws Exception {
        SerializableModel<SequentialBlock> model = myPartitionedModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        GraphStream gs = new GraphStream(env, (short)3);
        Dataset dataset = new CoraFull(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/datasets/cora"));
        DataStream<GraphOp>[] datasetStreamList = dataset.build(env);
        DataStream<GraphOp> partitioned = gs.partition(datasetStreamList[0], new HDRF());
        SingleOutputStreamOperator<GraphOp> trainTestSplit = partitioned.process(dataset.trainTestSplitter());
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(trainTestSplit, List.of(
                new TupleStorage()
                        .withPlugin(new GNNStreamingEmbeddingLayer(new SerializableModel<>("GNN-Layer-1",model.getBlock().getChildren().get(0).getValue()),true)),
                new TupleStorage()
                        .withPlugin(new GNNStreamingEmbeddingLayer(new SerializableModel<>("GNN-Layer-1",model.getBlock().getChildren().get(1).getValue()),true))
        ));

        DataStream<GraphOp> trainFeatures = trainTestSplit.getSideOutput(Dataset.TRAIN_TEST_DATA_OUTPUT);
        SingleOutputStreamOperator<GraphOp> output = gs.gnnLayerNewIteration(
                embeddings.union(trainFeatures),
                new TupleStorage()
                        .withPlugin(new VertexClassificationTester(new LossWrapper(Loss.softmaxCrossEntropyLoss())))
                        .withPlugin(new VertexClassificationLayer(new SerializableModel<>("GNN-Output",model.getBlock().getChildren().get(2).getValue())))
                );

        output.getSideOutput(VertexClassificationTester.TEST_LOSS_OUTPUT)
                .countWindowAll(100)
                .process(new ProcessAllWindowFunction<Float, Object, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Float, Object, GlobalWindow>.Context context, Iterable<Float> elements, Collector<Object> out) throws Exception {
                        float sum = 0;
                        for (Float element : elements) {
                            sum += element;
                        }
                        System.out.format("Loss is %s ",sum / 100);
                    }
                });


//        gs.gnnLayerNewIteration(embeddings, new TupleStorage().withPlugin(new VertexOutputInference(new SerializableModel<>("outputgnn", model.getBlock().getChildren().get(2).getValue()))));

////
//        DataStream<GraphOp> trainData = ((SingleOutputStreamOperator<GraphOp>) splittedData).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
//        DataStream<GraphOp> outputFunction = gs.gnnLayerNewIteration(embeddings.union(trainData), new TupleStorage().withPlugin(new PrintVertexPlugin("2262")).withPlugin(new EdgeOutputInference() {
//            @Override
//            public Model createOutputModel() {
//                SequentialBlock myBlock = new SequentialBlock();
//                myBlock.add(new LambdaBlock(inputs -> (
//                        new NDList(inputs.get(0).concat(inputs.get(1)))
//                )));
//                myBlock.add(Linear.builder().setUnits(16).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(32).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(1).build());
//                myBlock.add(Activation::sigmoid);
//                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(7));
//                Model model = Model.newInstance("prediction");
//                model.setBlock(myBlock);
//                return model;
//            }
//        }));
//
//        gs.gnnLoss(outputFunction, new SparseCategoricalCrossEntropyLoss(120) {
//            @Override
//            public Loss createLossFunction() {
//                return new BinaryCrossEntropy();
//            }
//        });


        env.execute();
    }
}
