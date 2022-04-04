package helpers;

import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import elements.GraphOp;
import functions.MovieLensStreamParser;
import functions.StreamingGNNLayerFunction;
import functions.TrainTestSplitter;
import org.apache.flink.streaming.api.datastream.DataStream;
import partitioner.HDRF;
import plugins.GNNLayerInference;
import plugins.GNNOutputEdgeInference;
import scala.Tuple2;
import storage.HashMapStorage;
//import plugins.GNNOutputTraining;


public class Main {


    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short) 5, (short) 2);

        DataStream<GraphOp> ratingsEdgeStream = gs.readSocket(new MovieLensStreamParser(), "localhost", 9090);
        DataStream<GraphOp> partitioned = gs.partition(ratingsEdgeStream, new HDRF());
        Tuple2<DataStream<GraphOp>, DataStream<GraphOp>> trainTestSplitted = gs.trainTestSplit(partitioned, new TrainTestSplitter());
        DataStream<GraphOp> normalData = trainTestSplitted._1.keyBy(new PartKeySelector()).map(item -> item).setParallelism(gs.layer_parallelism);

        DataStream<GraphOp> gnn1 = gs.gnnLayer(normalData, new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNLayerInference() {
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

        DataStream<GraphOp> gnn2 = gs.gnnLayer(gnn1, new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNLayerInference() {
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

        DataStream<GraphOp> trainingData = trainTestSplitted._2.keyBy(new PartKeySelector()).map(item -> item).setParallelism(gnn2.getParallelism());

        DataStream<GraphOp> predictions = gs.gnnLayer(gnn2.union(trainingData), new StreamingGNNLayerFunction(new HashMapStorage().withPlugin(new GNNOutputEdgeInference() {
            @Override
            public Model createOutputModel() {
                SequentialBlock myBlock = new SequentialBlock();
                myBlock.add(Linear.builder().setUnits(16).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(7).build());
                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
                Model model = Model.newInstance("prediction");
                model.setBlock(myBlock);
                return model;
            }
        })));

        gs.env.execute("HDRFPartitionerJOB");

    }
}
