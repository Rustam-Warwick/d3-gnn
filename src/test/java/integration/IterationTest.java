package integration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IterationTest extends IntegrationTest {
    private static final AtomicInteger sum = new AtomicInteger(0);

    @Test
    public void testIteration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        sum.set(0);
        env.setParallelism(1);
        DataStream<Integer> intStream = env.fromCollection(List.of(0));
        IterateStream<Integer, Integer> mapInts = IterateStream.startIteration(intStream.map(item -> item + 1));
        mapInts.closeIteration(mapInts.filter(i -> i < 10));
        mapInts.map(item -> {
            sum.set(sum.get() + item);
            return item;
        });
        env.execute();
        Assertions.assertEquals(55, sum.get());
    }
}
