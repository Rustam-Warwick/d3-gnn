package helpers;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.training.ParameterStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyParameterStore extends ParameterStore {
    public int MODEL_VERSION = 0;
    private Map<String, Parameter> parameterMap;
    private NDManager manager;
    public MyParameterStore() {

    }

    public MyParameterStore(NDManager manager, boolean copy) {
        this.manager = manager;
        this.parameterMap = new ConcurrentHashMap<>();
    }

    @Override
    public void updateAllParameters() {
        super.updateAllParameters();
    }

    @Override
    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        return super.getValue(parameter, device, training);
    }

    @Override
    public NDManager getManager() {
        return super.getManager();
    }

    @Override
    public void sync() {
        super.sync();
    }
}
