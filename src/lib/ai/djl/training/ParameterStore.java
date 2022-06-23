package ai.djl.training;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDHelper;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;

public abstract class ParameterStore {
    abstract public void updateAllParameters();

    abstract public NDArray getValue(Parameter parameter, Device device, boolean training);

    public NDManager getManager() {
        return NDHelper.globalNDManager;
    }

    abstract public void sync();

    public void setParameterServer() {
    }
}
