package plugins.edge_detection;

import ai.djl.Model;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.Edge;
import elements.Plugin;
import functions.nn.MyParameterStore;

import javax.annotation.Nonnull;
import java.util.Objects;

public abstract class EdgeOutputInference extends Plugin {
    public transient Model outputModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public boolean ACTIVE = true;

    public EdgeOutputInference() {
        super("inferencer");
    }

    public abstract Model createOutputModel();

    @Override
    public void add() {
        super.add();
        this.storage.withPlugin(new EdgeOutputTraining());
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
        this.parameterStore.setNDManager(BaseNDManager.threadNDManager.get());
    }

    @Override
    public void close() {
        super.close();
        this.outputModel.close();
    }

    public boolean outputReady(@Nonnull Edge edge) {
        return Objects.nonNull(edge.src.getFeature("feature")) && Objects.nonNull(edge.dest.getFeature("feature"));
    }

    public NDArray output(NDArray featureSource, NDArray featureDest, boolean training) {
        NDManager oldManagerSrc = featureSource.getManager();
        NDManager oldManagerDest = featureDest.getManager();
        featureSource.attach(BaseNDManager.threadNDManager.get());
        featureDest.attach(BaseNDManager.threadNDManager.get());
        NDArray res = this.outputModel.getBlock().forward(this.parameterStore, new NDList(featureSource, featureDest), training).get(0);
        featureSource.attach(oldManagerSrc);
        featureDest.attach(oldManagerDest);
        return res;
    }

}
