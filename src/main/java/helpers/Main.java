package helpers;

import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import elements.ElementType;
import elements.GraphOp;
import functions.GraphProcessFn;
import org.apache.flink.streaming.api.datastream.DataStream;
import partitioner.HDRF;
import plugins.GNNLayerInference;
import plugins.GNNLayerTraining;
import plugins.GNNOutputInference;
import plugins.GNNOutputTraining;


public class Main {
    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
        DataStream<GraphOp> dataset = gs.readTextFile(new EdgeStreamParser(new String[]{"Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
                "Probabilistic_Methods"}, "\t"), "/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/python/dataset/cora/merged.csv");
        DataStream<GraphOp> edges = dataset.filter(item->item.element.elementType() == ElementType.EDGE);
        DataStream<GraphOp> features = dataset.filter(item->item.element.elementType() == ElementType.VERTEX);

        DataStream<GraphOp> partitioned = gs.partition(edges, new HDRF());
        DataStream<GraphOp> gnn1 = gs.gnnLayer(partitioned, (GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference() {
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
                myBlock.add(new LambdaBlock(inputs->(
                        new NDList(inputs.get(0).concat(inputs.get(1)))
                )));
                myBlock.add(Linear.builder().setUnits(32).build());
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
        }).withPlugin(new GNNLayerTraining()));
        DataStream<GraphOp> gnn2 = gs.gnnLayer(gnn1, (GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference() {
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
                myBlock.add(new LambdaBlock(inputs->(
                        new NDList(inputs.get(0).concat(inputs.get(1)))
                )));
                myBlock.add(Linear.builder().setUnits(32).build());
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
        }).withPlugin(new GNNLayerTraining()));
        DataStream<GraphOp> predictions = gs.gnnLayer(gnn2, (GraphProcessFn) new GraphProcessFn().withPlugin(new GNNOutputInference() {
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
        }).withPlugin(new GNNOutputTraining()));

        gs.gnnLoss(predictions, features);
        gs.env.execute("HDRFPartitionerJOB");
    }
}
