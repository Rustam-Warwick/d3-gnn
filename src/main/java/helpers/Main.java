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
import functions.GraphProcessFn;
import org.apache.flink.streaming.api.datastream.DataStream;
import partitioner.HDRF;
import plugins.GNNLayerInference;
import plugins.GNNOutputInference;


public class Main {
    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(gs.env);

        DataStream<GraphOp> edges = gs.readTextFile(new EdgeStreamParser(new String[0]), "/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/python/dataset/cora/edges.csv");
        DataStream<GraphOp> features = gs.readTextFile(new EdgeStreamParser(new String[]{"Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
                "Probabilistic_Methods"}), "/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/python/dataset/cora/group-edges.csv");

        gs.partition(edges, new HDRF());
        gs.gnnLayer((GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference() {
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
        }));
        gs.gnnLayer((GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference() {
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
        }));
        gs.gnnLayer((GraphProcessFn) new GraphProcessFn().withPlugin(new GNNOutputInference() {

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
        }));
        gs.gnnLoss(features);
        System.out.println(gs.env.getExecutionPlan());
        gs.env.execute("HDRFPartitionerJOB");
    }
}
