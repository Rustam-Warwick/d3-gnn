package integration;

import ai.djl.BaseModel;
import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.gnn.AggregatorVariant;
import ai.djl.nn.gnn.HyperSAGEConv;
import ai.djl.nn.gnn.SAGEConv;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;

import java.util.ArrayList;

/**
 * Base class for all integration Tests
 */
abstract public class IntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(10)
                            .setNumberTaskManagers(3)
                            .build());

    /**
     * Get a multi-layered GNN {@link Model} with SUM {@link AggregatorVariant}
     */
    public static ArrayList<Model> getGNNModel(int layers) {
        SequentialBlock sb = new SequentialBlock();
        for (int i = 0; i < layers; i++) {
            SAGEConv layer = new SAGEConv(6, true);
            layer.setAgg(AggregatorVariant.SUM);
            sb.add(layer);
        }
        BaseModel model = (BaseModel) Model.newInstance("GNN");
        model.setBlock(sb);
        model.getBlock().initialize(BaseNDManager.getManager(), DataType.FLOAT32, new Shape(6));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            BaseModel tmp = (BaseModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });
        return models;
    }

    /**
     * Get a multi-layered HGNN Model
     */
    public static ArrayList<Model> getHGNNModel(int layers) {
        SequentialBlock sb = new SequentialBlock();
        for (int i = 0; i < layers; i++) {
            sb.add(new HyperSAGEConv(32, true));
        }
        BaseModel model = (BaseModel) Model.newInstance("GNN");
        model.setBlock(sb);
        model.getBlock().initialize(BaseNDManager.getManager(), DataType.FLOAT32, new Shape(128));
        ArrayList<Model> models = new ArrayList<>();
        sb.getChildren().forEach(item -> {
            BaseModel tmp = (BaseModel) Model.newInstance("GNN"); // Should all have the same name
            tmp.setBlock(item.getValue());
            models.add(tmp);
        });
        return models;
    }

}
