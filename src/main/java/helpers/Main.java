package helpers;

import elements.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Vertex> s = env.fromCollection(Arrays.asList(
                new Vertex("1111"),
                new Vertex("123"),
                new Vertex("1333"),
                new Vertex("133")
        ));

        s.map(vertex -> {
            vertex.setPartId((short) 1);
            return vertex;
        });
        s.print();


    }
}
