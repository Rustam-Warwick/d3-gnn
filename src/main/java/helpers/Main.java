package helpers;

import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import java.util.concurrent.locks.LockSupport;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.training.loss.Loss;
import elements.GraphOp;
import functions.MovieLensStreamParser;
import functions.StreamingGNNLayerFunction;
import functions.TrainTestSplitter;
import functions.loss.BinaryCrossEntropy;
import functions.loss.EdgeLossFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import partitioner.HDRF;
import plugins.GNNLayerInference;
import plugins.GNNOutputEdgeInference;
import storage.HashMapStorage;

public class Main {
    public static void main(String[] args) throws Exception {

        GraphStream gs = new GraphStream((short) 3, (short) 2);
        DataStream<GraphOp> ratingsEdgeStream = gs.readTextFile(new MovieLensStreamParser(), "/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/datasets/movielens/ratings.csv");
        DataStream<GraphOp> partitioned = gs.partition(ratingsEdgeStream, new HDRF());
        DataStream<GraphOp> splittedData = gs.trainTestSplit(partitioned, new TrainTestSplitter());

        DataStream<GraphOp> gnn1 = gs.gnnLayer(splittedData, new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNLayerInference() {
            @Override
            public Model createMessageModel() {
                SequentialBlock myBlock = new SequentialBlock();
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(64).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
                Model model = Model.newInstance("inference");
                model.setBlock(myBlock);
                return model;
            }

            @Override
            public Model createUpdateModel() {
                SequentialBlock myBlock = new SequentialBlock();
                myBlock.add(new LambdaBlock(inputs -> (
                        new NDList(inputs.get(0).concat(inputs.get(1)))
                )));
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(16).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(7).build());
                myBlock.add(Activation::relu);
                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(32));
                Model model = Model.newInstance("message");
                model.setBlock(myBlock);
                return model;
            }
        })));

//        DataStream<GraphOp> gnn2 = gs.gnnLayer(gnn1, new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNLayerInference() {
//            @Override
//            public Model createMessageModel() {
//                SequentialBlock myBlock = new SequentialBlock();
//                myBlock.add(Linear.builder().setUnits(32).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(64).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(32).build());
//                myBlock.add(Activation::relu);
//                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
//                Model model = Model.newInstance("inference");
//                model.setBlock(myBlock);
//                return model;
//            }
//
//            @Override
//            public Model createUpdateModel() {
//                SequentialBlock myBlock = new SequentialBlock();
//                myBlock.add(new LambdaBlock(inputs -> (
//                        new NDList(inputs.get(0).concat(inputs.get(1)))
//                )));
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(16).build());
//                myBlock.add(Activation::relu);
//                myBlock.add(Linear.builder().setUnits(7).build());
//                myBlock.add(Activation::relu);
//                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7), new Shape(32));
//                Model model = Model.newInstance("message");
//                model.setBlock(myBlock);
//                return model;
//            }
//        })));
//        // Submit Train Data as queries to be continuously pushed
//        DataStream<GraphOp> trainData = ((SingleOutputStreamOperator<GraphOp>)splittedData).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
//        DataStream<GraphOp> outputFunction = gs.gnnLayer(gnn2.union(trainData), new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNOutputEdgeInference() {
//            @Override
//            public Model createOutputModel(){
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
//        })));
//
//        DataStream<GraphOp> trainPredictionsData = ((SingleOutputStreamOperator<GraphOp>)outputFunction).getSideOutput(new OutputTag<>("training", TypeInformation.of(GraphOp.class)));
//        gs.gnnLoss(trainPredictionsData, new EdgeLossFunction(){
//            @Override
//            public Loss createLossFunction() {
//                return new BinaryCrossEntropy();
//            }
//        });


//        trainPredictionsData.print();

        gs.env.execute();
//        gs.env.execute();
//        gs.env.execute();
//        DataStream<GraphOp> trainingData = trainTestSplitted._2.keyBy(new PartKeySelector()).map(item -> item).setParallelism(gnn2.getParallelism());


    }
}
