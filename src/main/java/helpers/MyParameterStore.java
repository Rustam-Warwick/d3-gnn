package helpers;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.training.ParameterStore;
import ai.djl.training.initializer.ConstantInitializer;
import ai.djl.util.Pair;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyParameterStore extends ParameterStore implements Serializable {
    public int MODEL_VERSION = 0;
    public ParameterList parameterList;
    public transient Map<String, Parameter> parameterMap;
    public transient Map<String, NDArray> gradientMap;
    private transient NDManager manager;

    public MyParameterStore(){
        this(NDManager.newBaseManager());
    }
    public MyParameterStore(NDManager manager) {
        this.manager = manager;
        this.parameterList = new ParameterList();
        this.parameterMap = new ConcurrentHashMap<>();
        this.gradientMap = new ConcurrentHashMap<>();
    }


    public void appendList(ParameterList modelParams){
        modelParams.forEach(item->item.);
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

    private void writeObject(ObjectOutputStream oos) throws IOException {
        // default serialization
        for(Pair<String, Parameter> pair: this.parameterList){
            oos.writeBoolean(false);
            oos.writeUTF(pair.getKey());
            oos.writeUTF(pair.getValue().getName());
            pair.getValue().save(new DataOutputStream(oos));
        }
        oos.writeBoolean(true);

    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        // default deserialization
        NDManager manager = NDManager.newBaseManager();
        this.parameterList = new ParameterList();
        while(true){
            try {
                if(ois.readBoolean())break;
                String key = ois.readUTF();
                String name = ois.readUTF();
                Parameter tmp = new Parameter.Builder().setName(name).optInitializer(new ConstantInitializer(0)).build();
                tmp.load(manager, new DataInputStream(ois));
                this.parameterList.add(key, tmp);
            } catch (MalformedModelException e) {
                e.printStackTrace();
            }
        }

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
