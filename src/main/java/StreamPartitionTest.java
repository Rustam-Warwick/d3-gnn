import com.twitter.chill.java.ClosureSerializer;
import edge.SimpleEdge;
import features.Feature;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import part.BasePart;
import part.SimplePart;
import partitioner.BasePartitioner;
import partitioner.RandomPartitioning;
import types.GraphQuery;
import vertex.SimpleVertex;
import java.lang.invoke.SerializedLambda;


public class StreamPartitionTest {
    public static int parallelism = 2;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.registerType(SerializedLambda.class);
        env.registerType(SimpleVertex.class);
        env.registerType(GraphQuery.class);
        env.registerType(Feature.Update.class);
        env.registerTypeWithKryoSerializer(Nd4j.getBackend().getNDArrayClass(), Nd4jSerializer.class);
        env.registerTypeWithKryoSerializer(ClosureSerializer.Closure.class,ClosureSerializer.class);

        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
            String[] lineItems = item.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            INDArray v1A = Nd4j.ones(1);
//            v1.feature = new ReplicableTensorFeature("feature",v1,v1A);
//            v2.feature = new ReplicableTensorFeature("feature",v2,v1A);
            SimpleEdge<SimpleVertex> ed = new SimpleEdge<>(v1,v2);
            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
        }).setParallelism(1).name("Source Reader Mapper");

        DataStream<GraphQuery> partitionedStream = BasePartitioner.partitionHelper(source,new RandomPartitioning());

        DataStream<GraphQuery> levelZero = BasePart.partWithIteration(
                partitionedStream,
                new SimplePart<SimpleVertex>(),
                item->item.op == GraphQuery.OPERATORS.SYNC, // Sync Operators are sent back in iteration
                item->item.op!= GraphQuery.OPERATORS.SYNC // All other operators are going downstream
                );
        DataStream<GraphQuery> levelOne = BasePart.partWithIteration(
                levelZero,
                new SimplePart<SimpleVertex>(),
                item->item.op==GraphQuery.OPERATORS.SYNC,
                item->item.op!=GraphQuery.OPERATORS.SYNC
        );
        levelOne.print();


        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}
