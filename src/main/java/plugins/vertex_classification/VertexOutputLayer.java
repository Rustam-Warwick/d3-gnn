package plugins.vertex_classification;

import ai.djl.ndarray.NDList;
import elements.Plugin;
import plugins.ModelServer;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexOutputLayer extends Plugin {

    public final String modelName;
    public transient ModelServer modelServer;

    public VertexOutputLayer(String modelName) {
        super(String.format("%s-inferencer", modelName));
        this.modelName = modelName;
    }

    @Override
    public void open() {
        super.add();
        this.modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
    }

    public NDList output(NDList feature, boolean training) {
        return modelServer.getModel().getBlock().forward(modelServer.getParameterStore(), new NDList(feature), training);
    }

}
