import com.twitter.chill.java.ClosureSerializer;
import edge.SimpleEdge;
import features.Feature;
import features.ReplicableArrayListFeature;
import features.ReplicableTensorFeature;
import features.StaticFeature;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.nd4j.kryo.Nd4jSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.conditions.GreaterThan;
import part.BasePart;
import part.GNNPart;
import partitioner.BasePartitioner;
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
        StreamPartitionTest.registerSerializers(env);

        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
            String[] lineItems = item.split("\t");
            String id1 = lineItems[0];
            String id2 = lineItems[1];
            SimpleVertex v1 = new SimpleVertex(id1);
            SimpleVertex v2 = new SimpleVertex(id2);
            v1.feature = new ReplicableTensorFeature("feature",v1, Nd4j.rand(8,8));
            v2.feature = new ReplicableTensorFeature("feature",v2,Nd4j.rand(8,8));
            SimpleEdge<SimpleVertex> ed = new SimpleEdge<>(v1,v2);
            ed.feature = new StaticFeature<INDArray>("feature",ed,Nd4j.rand(8,8));
            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
        }).setParallelism(1).name("Source Reader Mapper");

        DataStream<GraphQuery> partitionedStream = BasePartitioner.partitionHelper(source,new RandomPartitioning());

        DataStream<GraphQuery> levelZero = BasePart.partWithIteration(
                partitionedStream,
                new GNNPart<SimpleVertex>((short)0,(short)1),
                item->{
                    return item.op == GraphQuery.OPERATORS.SYNC;
                }, // Sync Operators are sent back in iteration
                item->{
                    return item.op!= GraphQuery.OPERATORS.SYNC;}// All other operators are going downstream

                );

        DataStream<GraphQuery> levelOne = BasePart.partWithIteration(
                levelZero,
                new GNNPart<SimpleVertex>((short)1,(short)1),
                item->item.op==GraphQuery.OPERATORS.SYNC,
                item->item.op!=GraphQuery.OPERATORS.SYNC
        );

        levelOne.map(new RichMapFunction<GraphQuery, GraphQuery>() {
            private Meter meter;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.meter = getRuntimeContext().getMetricGroup().meter("myMeter",new MeterView(1));
            }

            @Override
            public GraphQuery map(GraphQuery value) throws Exception {
                this.meter.markEvent();
                if(value.element instanceof Feature.Update){
                    Feature.Update e = (Feature.Update) value.element;
                    System.out.format("%s -> %s\n",e.attachedId,e.value);
                    System.out.println(meter.getRate());
                }
                return value;
            }
        });

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

}
