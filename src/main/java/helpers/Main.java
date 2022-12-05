package helpers;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.hgnn.HSageConv;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Application entrypoint
 */
public class Main {
    // -d=tags-ask-ubuntu --tagsAskUbuntu:type=star-graph -p=hdrf --hdrf:lambda=1 -l=3 -f=true
    public static ArrayList<Model> layeredModel() {
        SequentialBlock sb = new SequentialBlock();
        sb.add(new HSageConv(64, true));
        sb.add(new HSageConv(47, true));
        sb.add(
                new SequentialBlock()
                        .add(Linear.builder().setUnits(47).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.relu(ndArrays);
                            }
                        })
                        .add(Linear.builder().setUnits(47).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.relu(ndArrays);
                            }
                        })
                        .add(Linear.builder().setUnits(47).optBias(true).build())
                        .add(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return Activation.softmax(ndArrays);
                            }
                        })

        );
        BaseModel model = (BaseModel) Model.newInstance("GNN");
        model.setBlock(sb);
//        NDHelper.loadModel(Path.of(System.getenv("DATASET_DIR"), "ogb-products", "graphSage"), model);
        model.getBlock().initialize(BaseNDManager.getManager(), DataType.FLOAT32, new Shape(100));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            BaseModel tmp = (BaseModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });
        return models;
    }

    public static void main(String[] args) throws Throwable {
        try {
            BaseNDManager.getManager().delay();
            ArrayList<Model> models = layeredModel(); // Get the model to be served
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<Integer> a = env.fromCollection(List.of(1,2,3,4,5,6,3,2,2,1,2,2,3,3));
            DataStream<Integer> b = env.fromCollection(List.of(1,2,3,4,5,6,3,2,2,1,2,2,3,3));
            DataStream<Integer> a_b = a.union(b);
            IterateStream<Integer, Integer> res = IterateStream.startIteration(a_b.keyBy(item -> item).map(item -> {
                System.out.println(item);
                return item;
            }));
            res.closeIteration(res.keyBy(item -> item));
            env.execute();
        } finally {
            BaseNDManager.getManager().resume();
        }
    }
}
