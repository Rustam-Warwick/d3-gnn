import com.twitter.chill.java.ClosureSerializer;
import datastream.GraphStream;
import edge.SimpleEdge;
import features.*;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.EagerSession;
import org.tensorflow.Tensor;
import org.tensorflow.internal.types.TFloat32Mapper;
import org.tensorflow.internal.types.TFloat64Mapper;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.op.Ops;
import org.tensorflow.op.Scope;
import org.tensorflow.op.core.Constant;
import org.tensorflow.op.random.RandomUniform;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.family.TType;
import partitioner.RandomPartitioning;
import serializers.TensorSerializer;
import types.GraphQuery;
import types.TFWrapper;
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
        env.registerTypeWithKryoSerializer(TFWrapper.class, TensorSerializer.class);
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(parallelism);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        StreamPartitionTest.registerSerializers(env);

        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
            Ops a = Ops.create();
            TFloat32 s1 = a.random.<TFloat32>randomUniform(a.constant(new int[]{2,2}),TFloat32.class).asTensor();
            TFloat32 s2 = a.random.<TFloat32>randomUniform(a.constant(new int[]{2,2}),TFloat32.class).asTensor();
            String[] lineItems = item.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            v1.feature = new ReplicableTFTensorFeature("feature",v1, new TFWrapper(s1));
            v2.feature = new ReplicableTFTensorFeature("feature",v2, new TFWrapper(s2));
            SimpleEdge ed = new SimpleEdge(v1,v2);
            ed.feature = new StaticFeature<TFWrapper<TFloat32>>("feature",ed,new TFWrapper(s2));
            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
        }).setParallelism(1).name("Source Reader Mapper");
        GraphStream stream = new GraphStream(source,env);
        GraphStream res = stream.partitionBy(new RandomPartitioning()).addGNN(0);
        res.input.map(item->{
            if(item.op== GraphQuery.OPERATORS.AGG){
                System.out.println("AGG");
            }
            return item;
        });




        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}
