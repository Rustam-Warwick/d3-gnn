package helpers;

import functions.GraphProcessFn;
import partitioner.RandomPartitioner;
import plugins.GNNLayerInference;


public class Main {


    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
        gs.readSocket(new EdgeStreamParser(new String[]{"Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
                "Probabilistic_Methods"}), "localhost", 9090 );
        gs.partition(new RandomPartitioner());
        gs.gnnLayer((GraphProcessFn) new GraphProcessFn().withPlugin(new GNNLayerInference()));
        System.out.println(gs.env.getExecutionPlan());
        gs.env.execute();
    }
}
