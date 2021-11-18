import com.twitter.chill.java.ClosureSerializer;
import datastream.GraphStreamBuilder;
import edge.SimpleEdge;
import features.ReplicableTensorFeature;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.common.primitives.AtomicDouble;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.nativeblas.Nd4jCpu;
import sources.GraphGenerator;
import sources.TSVFileStreamer;
import types.GraphQuery;
import vertex.SimpleVertex;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.util.logging.Logger;


public class StreamPartitionTest {

    public static void main(String[] args) throws Exception {


            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(50);
            env.registerType(SerializedLambda.class);
            env.registerTypeWithKryoSerializer(Nd4j.getBackend().getNDArrayClass(), Nd4jSerializer.class);
            env.registerTypeWithKryoSerializer(ClosureSerializer.Closure.class,ClosureSerializer.class);
            DataStream<GraphQuery> source =  env.readFile(new TextInputFormat(new Path("./src/main/resources/datasets/amazon0302_adj.tsv")),"./src/main/resources/datasets/amazon0302_adj.tsv").map(item->{
                String[] lineItems = item.split("\t");
                String id1 = lineItems[0];
                String id2 = lineItems[1];
                SimpleVertex v1 = new SimpleVertex(id1);
                SimpleVertex v2 = new SimpleVertex(id2);
                INDArray v1A = Nd4j.ones(4);
                v1.feature = new ReplicableTensorFeature("feature",v1,v1A);
                v2.feature = new ReplicableTensorFeature("feature",v2,v1A);
                SimpleEdge<SimpleVertex> ed = new SimpleEdge<>(v1,v2);
                GraphQuery q = new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
                return q;
            });
//        SingleOutputStreamOperator<GraphQuery> source = env.addSource(new TSVFileStreamer("./src/main/resources/datasets/amazon0302_adj.tsv"));

            new GraphStreamBuilder(source,env)
                    .startAccepting(GraphStreamBuilder.PARTITIONER.RANDOM)
                    .build();
        env.execute();
        System.out.println(env.getStreamGraph());
    }

}
