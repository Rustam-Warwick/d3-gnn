import com.twitter.chill.java.ClosureSerializer;
import datastream.GraphStream;
import edge.SimpleEdge;
import features.Feature;
import features.ReplicableArrayListFeature;
import features.ReplicableTensorFeature;
import features.StaticFeature;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import partitioner.RandomPartitioning;
import types.GraphQuery;
import vertex.SimpleVertex;

import java.lang.invoke.SerializedLambda;


public class StreamPartitionTest {
    public static int parallelism = 2;
    public static void registerSerializers(StreamExecutionEnvironment env){
        env.registerType(SerializedLambda.class);
        env.registerType(SimpleVertex.class);
        env.registerType(Feature.Update.class);
        env.registerType(SimpleEdge.class);
        env.registerType(ReplicableArrayListFeature.class);
        env.registerType(ReplicableTensorFeature.class);
        env.registerTypeWithKryoSerializer(Nd4j.getBackend().getNDArrayClass(), Nd4jSerializer.class);
        env.registerTypeWithKryoSerializer(ClosureSerializer.Closure.class,ClosureSerializer.class);
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        StreamPartitionTest.registerSerializers(env);

        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
            String[] lineItems = item.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            v1.feature = new ReplicableTensorFeature("feature",v1, Nd4j.rand(8,8));
            v2.feature = new ReplicableTensorFeature("feature",v2,Nd4j.rand(8,8));
            SimpleEdge ed = new SimpleEdge(v1,v2);
            ed.feature = new StaticFeature<INDArray>("feature",ed,Nd4j.rand(8,8));
            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
        }).setParallelism(1).name("Source Reader Mapper");
        GraphStream stream = new GraphStream(source,env);
        stream.partitionBy(new RandomPartitioning()).addGNN(1);




        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}
