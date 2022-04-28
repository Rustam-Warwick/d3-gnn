package plugins.vertex_classification;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.Plugin;
import elements.Vertex;
import features.VTensor;
import helpers.MyParameterStore;

import java.util.Objects;

public abstract class VertexOutputInference extends Plugin {
    public transient Model outputModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public int MODEL_VERSION = 0;
    public boolean updatePending = false;

    public VertexOutputInference() {
        super("inferencer");
    }

    public abstract Model createOutputModel();

    @Override
    public void add() {
        super.add();
        this.storage.withPlugin(new VertexOutputTraining());
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.loadModel(this.outputModel);
    }

    @Override
    public void open() {
        super.open();
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.restoreModel(this.outputModel);
        this.parameterStore.setNDManager(this.storage.manager.getLifeCycleManager());
    }

    @Override
    public void close() {
        super.close();
        this.outputModel.close();
    }

    public boolean outputReady(Vertex v) {
        return !updatePending && Objects.nonNull(v.getFeature("feature")) && ((VTensor) v.getFeature("feature")).isReady(MODEL_VERSION);
    }

    public NDArray output(NDArray feature, boolean training) {
        NDManager oldManager = feature.getManager();
        feature.attach(this.storage.manager.getTempManager());
        NDArray res = this.outputModel.getBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        feature.attach(oldManager);
        return res;
    }

}
