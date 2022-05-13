package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Block;
import elements.Plugin;
import elements.Vertex;
import functions.nn.MyParameterStore;
import ai.djl.ndarray.SerializableModel;

import java.util.Objects;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexOutputLayer extends Plugin {
    public SerializableModel<Block> model;
    public transient MyParameterStore parameterStore;

    public VertexOutputLayer(SerializableModel<Block> model) {
        super("inferencer");
        this.model = model;
    }

    @Override
    public void open() {
        super.add();
        parameterStore = new MyParameterStore(storage.manager.getLifeCycleManager());
        model.setManager(storage.manager.getLifeCycleManager());
        parameterStore.loadModel(this.model);
    }

    @Override
    public void close() {
        super.close();
        model.close();
    }

    public boolean outputReady(Vertex v) {
        return Objects.nonNull(v.getFeature("feature"));
    }

    public NDArray output(NDArray feature, boolean training) {
        NDManager oldManager = feature.getManager();
        feature.attach(this.storage.manager.getTempManager());
        NDArray res = this.model.getBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        feature.attach(oldManager);
        return res;
    }

}
