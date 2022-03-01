package helpers;

import elements.GraphElement;
import elements.GraphOp;
import functions.GraphProcessFn;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import partitioner.RandomPartitioner;


public class Main {

    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
        gs.readSocket(new EdgeStreamParser(new String[]{"Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
                "Probabilistic_Methods"}), "localhost", 9090 );
        gs.partition(new RandomPartitioner());
        gs.gnnLayer(new GraphProcessFn());
        gs.last.print();
        gs.env.execute();
    }
}
