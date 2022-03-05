package helpers;

import ai.djl.Device;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.training.ParameterServer;
import ai.djl.training.ParameterStore;
import storage.BaseStorage;

public class MyParameterStore extends ParameterStore {

    public MyParameterStore() {

    }

    public MyParameterStore(NDManager manager, boolean copy) {
        super(manager, copy);
        this.setParameterServer(new MyParameterServer());
    }

    public void setParameterServer(ParameterServer parameterServer) {
        super.setParameterServer(parameterServer, BaseStorage.tensorManager.getEngine().getDevices());
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
