package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Block;
import elements.ElementType;
import elements.GraphElement;
import elements.Plugin;
import elements.Vertex;
import features.Tensor;
import functions.nn.MyParameterStore;
import functions.nn.SerializableModel;

import java.util.Objects;

public class VertexOutputInference extends Plugin {
    public SerializableModel<Block> model;
    public transient MyParameterStore parameterStore;

    public VertexOutputInference(SerializableModel<Block> model) {
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
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Tensor feature = (Tensor) newElement;
            if ("feature".equals(feature.getName())) {
                forward(feature);
            }
        }
    }

    public void forward(Tensor feature) {
        NDArray update = output(feature.getValue(), false);
        System.out.println(update);
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
