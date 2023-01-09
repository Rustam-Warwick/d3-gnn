package plugins.vertex_classification;

import ai.djl.ndarray.NDList;
import elements.Plugin;
import org.apache.flink.configuration.Configuration;
import plugins.ModelServer;

/**
 * Base class for all Vertex output Plugins
 */
abstract public class BaseVertexOutput extends Plugin {

    public final String modelName;

    public transient ModelServer<?> modelServer;

    public BaseVertexOutput(String modelName, String suffix) {
        super(String.format("%s-%s", modelName, suffix));
        this.modelName = modelName;
    }

    public BaseVertexOutput(String modelName, String suffix, boolean IS_ACTIVE) {
        super(String.format("%s-%s", modelName, suffix), IS_ACTIVE);
        this.modelName = modelName;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        modelServer = (ModelServer) getRuntimeContext().getPlugin(String.format("%s-server", modelName));
    }

    public NDList output(NDList feature, boolean training) {
        return modelServer.getModel().getBlock().forward(modelServer.getParameterStore(), feature, training);
    }

}
