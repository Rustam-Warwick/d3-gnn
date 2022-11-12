package ai.djl.training;

import ai.djl.Device;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;

public class ParameterStore {
    public ParameterStore() {

    }

    public ParameterStore(NDManager manager, boolean copy) {

    }

    public void updateAllParameters() {
    }

    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        return null;
    }

    public NDManager getManager() {
        return BaseNDManager.getManager();
    }

    public void sync() {
    }

    public void setParameterServer() {
    }

}
