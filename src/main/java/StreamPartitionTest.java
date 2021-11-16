import com.twitter.chill.java.ClosureSerializer;
import datastream.GraphStreamBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.common.primitives.AtomicDouble;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.nativeblas.Nd4jCpu;
import sources.GraphGenerator;
import types.GraphQuery;
import java.lang.invoke.SerializedLambda;
import java.util.logging.Logger;


public class StreamPartitionTest {

    public static void main(String[] args) throws Exception {


            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerType(SerializedLambda.class);
            env.registerTypeWithKryoSerializer(Nd4j.getBackend().getNDArrayClass(), Nd4jSerializer.class);
            env.registerTypeWithKryoSerializer(ClosureSerializer.Closure.class,ClosureSerializer.class);


            SingleOutputStreamOperator<GraphQuery> source = env.addSource(new GraphGenerator());

            new GraphStreamBuilder(source,env)
                    .startAccepting(GraphStreamBuilder.PARTITIONER.RANDOM)
                    .build();
        env.executeAsync();
        System.out.println(env.getStreamGraph());
    }


}
