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
    public Map<String, Parameter> parameterMap;
    public Map<String, NDArray> gradientMap;
    private NDManager manager;
    public MyParameterStore() {

    }

    public MyParameterStore(NDManager manager) {
        this.manager = manager;
        this.parameterMap = new ConcurrentHashMap<>();
        this.gradientMap = new ConcurrentHashMap<>();
    }

    @Override
    public void updateAllParameters() {
        System.out.println("Update all parameters");

    }

    @Override
    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        this.parameterMap.putIfAbsent(parameter.getId(), parameter);
        this.gradientMap.putIfAbsent(parameter.getId(), this.manager.zeros(parameter.getArray().getShape()));
        NDArray valueParam = this.parameterMap.get(parameter.getId()).getArray();
        if(valueParam.hasGradient() && !training){
            this.gradientMap.get(parameter.getId()).addi(valueParam.getGradient());
            valueParam.setRequiresGradient(training);
        }
        else if(!valueParam.hasGradient() && training){
            valueParam.setRequiresGradient(training);
        }

        return valueParam;
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
