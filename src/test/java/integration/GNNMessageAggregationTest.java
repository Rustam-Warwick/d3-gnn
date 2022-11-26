package integration;

import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import elements.GraphOp;
import functions.storage.StreamingStorageProcessFunction;
import helpers.GraphStream;
import helpers.datasets.MeshGraphGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import plugins.ModelServer;
import plugins.hgnn_embedding.SessionWindowedHGNNEmbeddingLayer;
import storage.FlatObjectStorage;

import java.util.ArrayList;

public class GNNMessageAggregationTest extends IntegrationTest{

    @ParameterizedTest
    @ValueSource(strings = {"-p=hdrf -l=2"})
    void startCluster(String argsString) throws Exception{
        try {
            BaseNDManager.getManager().delay();
            String[] args = argsString.split(" ");
            ArrayList<Model> models = getGNNModel(2); // Get the model to be served
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<GraphOp>[] gs = new GraphStream(env, args, true, false, false,
                    new StreamingStorageProcessFunction(new FlatObjectStorage()
                            .withPlugin(new ModelServer<>(models.get(0)))
//                            .withPlugin(new StreamingHGNNEmbeddingLayer(models.get(0).getName(), true))
                            .withPlugin(new SessionWindowedHGNNEmbeddingLayer(models.get(0).getName(), true, 50))
                    )
            ).setDataset(new MeshGraphGenerator(10)).build();
            env.execute();
        } finally {
            BaseNDManager.getManager().resume();
        }
    }

}
