package plugins.vertex_classification;

import ai.djl.ndarray.NDList;
import elements.Plugin;
import org.apache.flink.configuration.Configuration;
import plugins.ModelServer;

/**
 * Base class for all Vertex output Plugins
 */
abstract public class BaseVertexOutputPlugin extends Plugin {

    public final String modelName;

    public transient ModelServer<?> modelServer;

    public BaseVertexOutputPlugin(String modelName, String suffix) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
    }

    public BaseVertexOutputPlugin(String modelName, String suffix, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix), IS_ACTIVE);
        this.modelName = modelName;
    }

    @Override
    public synchronized void open(Configuration params) throws Exception {
        super.open(params);
        modelServer = modelServer == null ? (ModelServer) getRuntimeContext().getPlugin(String.format("%s-server", modelName)): null;
    }

    public NDList output(NDList feature, boolean training) {
        return modelServer.getModel().getBlock().forward(modelServer.getParameterStore(), feature, training);
    }

}
