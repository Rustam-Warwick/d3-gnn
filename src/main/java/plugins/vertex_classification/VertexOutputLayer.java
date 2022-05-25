package plugins.vertex_classification;

import ai.djl.ndarray.NDList;
import ai.djl.training.ParameterStore;
import elements.Plugin;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexOutputLayer extends Plugin {

    public transient ParameterStore modelServer;
    public final String modelName;
    public VertexOutputLayer(String modelName) {
        super(String.format("%s-inferencer",modelName));
        this.modelName = modelName;
    }

    @Override
    public void open() {
        super.add();
        this.modelServer = (ParameterStore) storage.getPlugin(String.format("%s-server", modelName));
    }

    public NDList output(NDList feature, boolean training) {
        return modelServer.getModel().getBlock().forward(modelServer, new NDList(feature), training);
    }

}
