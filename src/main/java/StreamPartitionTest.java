import com.twitter.chill.java.ClosureSerializer;
import datastream.GraphStreamBuilder;
import edge.SimpleEdge;
import features.ReplicableTensorFeature;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import part.SimplePart;
import partitioner.BasePartitioner;
import partitioner.RandomPartitioning;
import types.GraphQuery;
import vertex.SimpleVertex;

import javax.xml.crypto.Data;
import java.lang.invoke.SerializedLambda;


public class StreamPartitionTest {
    public static int parallelism = 3;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            env.getConfig().disableClosureCleaner();
        env.setParallelism(parallelism);
        env.registerType(SerializedLambda.class);
        env.registerTypeWithKryoSerializer(Nd4j.getBackend().getNDArrayClass(), Nd4jSerializer.class);
        env.registerTypeWithKryoSerializer(ClosureSerializer.Closure.class,ClosureSerializer.class);

        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
            String[] lineItems = item.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            INDArray v1A = Nd4j.ones(1);
            v1.feature = new ReplicableTensorFeature("feature",v1,v1A);
            v2.feature = new ReplicableTensorFeature("feature",v2,v1A);
            SimpleEdge<SimpleVertex> ed = new SimpleEdge<>(v1,v2);
            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
        }).name("Source Reader Mapper");
        DataStream<GraphQuery> partitioned_map = source.map(new RandomPartitioning()).setParallelism(1).map(item->item).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
        IterativeStream<GraphQuery> iterationStart = partitioned_map.iterate();
        DataStream<GraphQuery> partitionedIterations = iterationStart.partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());

        DataStream<GraphQuery> afterPart =  partitionedIterations.process(new SimplePart<SimpleVertex>());
        iterationStart.closeWith(afterPart);


        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}
