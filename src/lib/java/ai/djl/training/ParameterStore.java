package ai.djl.training;

import ai.djl.Device;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Objects;

public class ParameterStore {
    public ParameterStore() {

    }

    public ParameterStore(NDManager manager, boolean copy) {

    }

    public void updateAllParameters() {
        throw new NotImplementedException("Not supported by default");
    }

    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        if (Objects.nonNull(parameter)) {
            if (parameter.getArray().hasGradient() != training) {
                parameter.getArray().setRequiresGradient(training);
            }
            return parameter.getArray();
        } else {
            return null;
        }
    }

    public NDManager getManager() {
        return BaseNDManager.getManager();
    }

    public void sync() {
        throw new NotImplementedException("Not supported by default");
    }

}
