package helpers;

import ai.djl.MalformedModelException;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import datasets.CoraFull;
import datasets.Dataset;
import elements.GraphOp;
import functions.nn.MyActivations;
import functions.nn.SerializableModel;
import functions.nn.gnn.SAGEConv;
import functions.splitter.EdgeTrainTestSplitter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.HDRF;
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

        model.load(Path.of("/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/jupyter/models/GraphSage-CoraFull-2022-05-08"));
        model.getBlock().initialize(model.getNDManager(), DataType.FLOAT32, new Shape(8710));
        return model;
    }

    public static void main(String[] args) throws Exception {
//        SerializableModel<SequentialBlock> model = myPartitionedModel();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        GraphStream gs = new GraphStream(env, (short)3);
        Dataset dataset = new CoraFull(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/datasets/cora/edges"), Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/jupyter/datasets/cora/vertices"));
        DataStream<GraphOp> partitioned = gs.partition(dataset.build(env), new HDRF());
        DataStream<GraphOp> splittedData = gs.trainTestSplit(partitioned, new EdgeTrainTestSplitter(0.005));
        DataStream<GraphOp> embeddings = gs.gnnEmbeddings(splittedData, List.of(
                new TupleStorage(),
                new TupleStorage()
        ));
//        gs.gnnLayerNewIteration(embeddings, new TupleStorage().withPlugin(new VertexOutputInference(new SerializableModel<>("outputgnn", model.getBlock().getChildren().get(2).getValue()))));
        embeddings.print();

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
