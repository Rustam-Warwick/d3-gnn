package helpers;

import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.mxnet.engine.MxModel;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.*;
import ai.djl.nn.core.Linear;
import ai.djl.nn.core.Prelu;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.Translator;
import functions.GraphProcessFn;
import partitioner.RandomPartitioner;
import plugins.GNNLayerInference;

import java.io.IOException;
import java.nio.file.Path;


public class Main {

    public static Model generateInferenceFunction(){
        SequentialBlock myBlock = new SequentialBlock();
        myBlock.add(Linear.builder().setUnits(32).build());
        myBlock.add(Activation::relu);
        myBlock.add(Linear.builder().setUnits(16).build());
        myBlock.add(Activation::relu);
        myBlock.add(Linear.builder().setUnits(7).build());
        myBlock.add(Activation::relu);
        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(37));
        Model model = Model.newInstance("message");
        model.setBlock(myBlock);
        try {
            model.save(Path.of("/Users/rustamwarwick/Documents/Projects/Flink-Partitioning/python/dataset/"),"model");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return model;
    }

    public static Model generateMessageFunction(){
        SequentialBlock myBlock = new SequentialBlock();

        myBlock.add(Linear.builder().setUnits(32).build());
        myBlock.add(Activation::relu);
        myBlock.add(Linear.builder().setUnits(32).build());
        myBlock.add(Activation::relu);
        myBlock.add(Linear.builder().setUnits(32).build());
        myBlock.add(Activation::relu);
        myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(7));
        Model model = Model.newInstance("inference");
        model.setBlock(myBlock);
        return model;
    }

    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
        gs.readSocket(new EdgeStreamParser(new String[]{"Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
                "Probabilistic_Methods"}), "localhost", 9090 );
        gs.partition(new RandomPartitioner());
        gs.gnnLayer((GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference() {
            @Override
            public Model createMessageModel() {
                SequentialBlock myBlock = new SequentialBlock();
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(32).build());
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
                myBlock.add(Linear.builder().setUnits(32).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(16).build());
                myBlock.add(Activation::relu);
                myBlock.add(Linear.builder().setUnits(7).build());
                myBlock.add(Activation::relu);
                myBlock.initialize(NDManager.newBaseManager(), DataType.FLOAT32, new Shape(37));
                Model model = Model.newInstance("message");
                model.setBlock(myBlock);
                return model;
            }
        }));
        System.out.println(gs.env.getExecutionPlan());
        gs.env.execute();
    }
}
