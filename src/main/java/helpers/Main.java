package helpers;

import elements.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.RandomPartitioner;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws Exception {
        GraphStream gs = new GraphStream((short)5, (short)2);
        gs.readSocket(new EdgeStreamParser(), "localhost", 9090 );
        gs.partition(new RandomPartitioner());

    }
}
