import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.bytedeco.javacpp.annotation.Raw;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.nd4j.linalg.api.ops.impl.reduce.same.Min;
import scala.Int;
import scala.Tuple2;
import scala.Tuple4;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class StreamPartitionTest {
    public static int parallelism = 2;

    public static class RawInteraction{
        public int user_id;
        public int campaign_id;
        public int num_interactions;
        public int[] durations;
        public int[] start_dates;

        public RawInteraction(int user_id, int campaign_id, int num_interactions, int[] durations, int[] start_dates) {
            this.user_id = user_id;
            this.campaign_id = campaign_id;
            this.num_interactions = num_interactions;
            this.durations = durations;
            this.start_dates = start_dates;
        }

        @Override
        public String toString() {
            return "RawInteraction{" +
                    "user_id=" + user_id +
                    ", campaign_id=" + campaign_id +
                    ", num_interactions=" + num_interactions +
                    ", durations=" + Arrays.toString(durations) +
                    ", start_dates=" + Arrays.toString(start_dates) +
                    '}';
        }
    }

    public static class Interaction{
        public int user_id;
        public int campaign_id;
        public int duration;
        public int start_date;
        public Interaction(){
            user_id = 0;
            campaign_id = 0;
            duration = 0;
            start_date = 0;
        }
        public Interaction(int user_id, int campaign_id, int durations, int start_date) {
            this.user_id = user_id;
            this.campaign_id = campaign_id;
            this.duration = durations;
            this.start_date = start_date;
        }

        @Override
        public String toString() {
            return "Interaction{" +
                    "user_id=" + user_id +
                    ", campaign_id=" + campaign_id +
                    ", duration=" + duration +
                    '}';
        }
    }

    public static class MinMeanMaxMapper extends RichMapFunction<Interaction, Tuple4<String, Float, Float,Float>>{
        private transient ValueState<Tuple2<Float, Integer>> mean;
        private transient ValueState<Float> max;
        private transient ValueState<Float> min;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Float, Integer>> meandesc = new ValueStateDescriptor<Tuple2<Float, Integer>>(
                    "mean",
                    TypeInformation.of( new TypeHint<Tuple2<Float,Integer>>(){}),
                    new Tuple2<>(0f, 0)
            );
            ValueStateDescriptor<Float> maxdesc = new ValueStateDescriptor<Float>(
                    "max",
                    TypeInformation.of( new TypeHint<Float>(){}),
                    Float.NEGATIVE_INFINITY
            );
            ValueStateDescriptor<Float> mindesc = new ValueStateDescriptor<Float>(
                    "min",
                    TypeInformation.of( new TypeHint<Float>(){}),
                    Float.POSITIVE_INFINITY
            );
            mean = getRuntimeContext().getState(meandesc);
            max = getRuntimeContext().getState(maxdesc);
            min = getRuntimeContext().getState(mindesc);
        }

        @Override
        public Tuple4<String, Float, Float, Float> map(Interaction value) throws Exception {
             Tuple2<Float, Integer> curMean = this.mean.value();
             Tuple2<Float, Integer> newMean =
                     new Tuple2<>((curMean._1 * curMean._2 + value.duration)/(curMean._2 + 1),
                     curMean._2 + 1);
             this.mean.update(newMean);
             Float max = this.max.value();
             if(value.duration > max){
                 max = Float.valueOf(value.duration);
                 this.max.update(max);
             }
             Float min = this.min.value();
             if(value.duration < min){
                 min = Float.valueOf(value.duration);
                 this.min.update(min);
             }

             return new Tuple4<>(value.user_id + ":" + value.campaign_id, min, newMean._1, max );
        }
    }

    public static void test(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        ArrayList<RawInteraction> list = new ArrayList<>();
//        list.add(new RawInteraction(1,1,4, new int[]{30, 40, 20, 100}, new int[]{0, 3000, 2000, 6000}));
//        list.add(new RawInteraction(2,1,4, new int[]{30, 40, 20, 22}, new int[]{1000, 3000, 9000, 3000}));
        list.add(new RawInteraction(3,1,3, new int[]{30,40,20}, new int[]{5000, 20000, 9000}));

        DataStream<RawInteraction> interactions = env.fromCollection(list); // Ingest Data
        interactions.print(); // Print for debugging
        DataStream<RawInteraction> filteredInteractions =
               interactions.filter(item -> (item.num_interactions > 0) && item.durations.length == item.num_interactions);
                // Filter faulty Values
        DataStream<Interaction> atomicInteractions = filteredInteractions.flatMap(new FlatMapFunction<RawInteraction, Interaction>() {
            @Override
            public void flatMap(RawInteraction value, Collector<Interaction> out) throws Exception {
                for (int i=0; i<value.num_interactions; i++){
                    Interaction inter = new Interaction(value.user_id, value.campaign_id, value.durations[i]* 1000,value.start_dates[i]);
                    out.collect(inter);
                }
            }
        }); // Map each RawInteraction to 1-Many Interactions
        WatermarkStrategy<Interaction> strategy = WatermarkStrategy.
                <Interaction>forBoundedOutOfOrderness(Duration.ofSeconds(5)).
                withTimestampAssigner((element, recordTimestamp) -> element.start_date);
        DataStream<Interaction> withTimeStamps = atomicInteractions.assignTimestampsAndWatermarks(strategy);

        KeyedStream<Interaction, String> keyedInteractions =
                withTimeStamps.keyBy(item -> item.user_id + ":" + item.campaign_id);
                // Keyby the user_id:campain_id
        keyedInteractions.window(TumblingEventTimeWindows.of(Time.seconds(3))).maxBy("duration").print();

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        test(args);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().disableClosureCleaner();
//        env.setParallelism(parallelism);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        StreamPartitionTest.registerSerializers(env);
//
//        DataStream<GraphQuery> source =  env.socketTextStream("127.0.0.1",9090).setParallelism(1).map(item->{
//            Ops a = Ops.create();
//            TFloat32 s1 = a.random.<TFloat32>randomUniform(a.constant(new int[]{2,2}),TFloat32.class).asTensor();
//            TFloat32 s2 = a.random.<TFloat32>randomUniform(a.constant(new int[]{2,2}),TFloat32.class).asTensor();
//            String[] lineItems = item.split("\t");
//            String id1 = lineItems[0];
//            String id2 = lineItems[1];
//            SimpleVertex v1 = new SimpleVertex(id1);
//            SimpleVertex v2 = new SimpleVertex(id2);
//            v1.feature = new ReplicableTFTensorFeature("feature",v1, new TFWrapper(s1));
//            v2.feature = new ReplicableTFTensorFeature("feature",v2, new TFWrapper(s2));
//            SimpleEdge ed = new SimpleEdge(v1,v2);
//            ed.feature = new StaticFeature<TFWrapper<TFloat32>>("feature",ed,new TFWrapper(s2));
//            return new GraphQuery(ed).changeOperation(GraphQuery.OPERATORS.ADD);
//        }).setParallelism(1).name("Source Reader Mapper");
//        GraphStream stream = new GraphStream(source,env);
//        GraphStream res = stream.partitionBy(new RandomPartitioning()).addGNN(0);
//        res.input.map(item->{
//            if(item.op== GraphQuery.OPERATORS.AGG){
//                System.out.println("AGG");
//            }
//            return item;
//        });
//
//
//
//
//        System.out.println(env.getExecutionPlan());
//        env.execute();
    }

}
