package helpers;

import ai.djl.ndarray.NDArray;
import ai.djl.training.ParameterServer;

import java.util.HashMap;

public class MyParameterServer implements ParameterServer {
    public HashMap<String, NDArray> parameters = new HashMap();
    @Override
    public void init(String parameterId, NDArray[] value) {
        this.parameters.putIfAbsent(parameterId, value[0]);
    }

    @Override
    public void update(String parameterId, NDArray[] grads, NDArray[] params) {

    }

    @Override
    public void close() {

    }

    @Override
    public void update(String parameterId, NDArray[] params) {
        ParameterServer.super.update(parameterId, params);
    }
}
