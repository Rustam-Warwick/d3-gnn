package plugins.vertex_classification;

import ai.djl.ndarray.NDList;
import elements.Plugin;
import plugins.ModelServer;

/**
 * Base class for all Vertex output Plugins
 */
abstract public class BaseVertexOutputPlugin extends Plugin {

    public final String modelName;

    public boolean IS_ACTIVE;

    public transient ModelServer modelServer;

    public BaseVertexOutputPlugin(String modelName, String suffix) {
        this(modelName, suffix, true);
    }

    public BaseVertexOutputPlugin(String modelName, String suffix, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
        this.IS_ACTIVE = IS_ACTIVE;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
    }

    public NDList output(NDList feature, boolean training) {
        return modelServer.getModel().getBlock().forward(modelServer.getParameterStore(), new NDList(feature), training);
    }

}
