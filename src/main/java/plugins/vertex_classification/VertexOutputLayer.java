package plugins.vertex_classification;

import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDList;
import elements.Plugin;
import functions.nn.MyParameterStore;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexOutputLayer extends Plugin {
    public Model model;
    public transient MyParameterStore parameterStore;

    public VertexOutputLayer(Model model) {
        super("inferencer");
        this.model = model;
    }

    @Override
    public void open() {
        super.add();
        parameterStore = new MyParameterStore(BaseNDManager.threadNDManager.get());
        parameterStore.loadModel(this.model);
    }

    @Override
    public void close() {
        super.close();
        model.close();
    }
    public NDList output(NDList feature, boolean training) {
        return model.getBlock().forward(this.parameterStore, new NDList(feature), training);
    }

}
