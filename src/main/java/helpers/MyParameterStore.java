package helpers;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.training.ParameterStore;
import ai.djl.util.Pair;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyParameterStore extends ParameterStore implements Serializable {
    public Map<String, NDArray> parameterArrays;
    public Map<String, NDArray> gradientArrays;

    private transient NDManager manager;

    public MyParameterStore(){
        this(NDManager.newBaseManager());
    }
    public MyParameterStore(NDManager manager) {
        this.manager = manager;
        this.parameterArrays = new ConcurrentHashMap<>();
        this.gradientArrays = new HashMap<>();
    }

    public void setNDManager(NDManager newManager){
        this.parameterArrays.forEach((id, value)->{value.attach(newManager);});
        this.gradientArrays.forEach((id, value)->{value.attach(newManager);});
        manager.close();
        this.manager = newManager;
    }

    /**
     * Subtracts grads from parameter arrays
     */
    public void step() {
        parameterArrays.forEach((key, item)->{
            item.setRequiresGradient(false);
            NDArray grad = gradientArrays.get(key);
            item.subi(grad);
        });
    }

    public void updateParameters(Map<String, NDArray> newParams){
        parameterArrays.putAll(newParams);
    }

    /**
     * Change ids of model parameters to a single standard
     * @param model
     */
    public void canonizeModel(Model model){
        String modelName = model.getName();
        ParameterList params = model.getBlock().getParameters();
        try {
            Field idField = Parameter.class.getDeclaredField("id");
            idField.setAccessible(true);
            for (Pair<String, Parameter> param : params) {
                idField.set(param.getValue(), modelName+param.getKey()+param.getValue().getName());
            }
        }catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Load the Model to ParameterStore intially
     * @param model
     */
    public void loadModel(Model model){
        model.getBlock().getParameters().forEach(item->{
            this.parameterArrays.putIfAbsent(item.getValue().getId(), item.getValue().getArray());
            this.gradientArrays.putIfAbsent(item.getValue().getId(), item.getValue().getArray().zerosLike());
        });
    }

    /**
     * Restore the model parameters saved here
     * @param model
     */
    public void restoreModel(Model model){
        model.getBlock().getParameters().forEach(item->{
            NDArray thisArray = this.parameterArrays.get(item.getValue().getId());
            item.getValue().close();
            item.getValue().setShape(null);
            item.getValue().setArray(thisArray);
        });

    }

    public void addGrads(Map<String, NDArray> newGrads){
        this.gradientArrays.forEach((key,items)->{
            NDArray x = newGrads.get(key);
            items.addi(x);
        });
    }

    /**
     * Nullify gradients
     */
    public void resetGrads(){
        this.gradientArrays.forEach((key, item)->{
            item.subi(item);
        });
    }

    @Override
    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        NDArray valueParam = this.parameterArrays.get(parameter.getId());
        if(valueParam.hasGradient() && !training){
            NDArray grad = valueParam.getGradient();
            this.gradientArrays.compute(parameter.getId(), (key, value)->value.addi(grad) ); // Commit accumulator changes
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


    private void writeObject(ObjectOutputStream oos) throws IOException {
        DataOutputStream dos = new DataOutputStream(oos);
        dos.writeInt(this.parameterArrays.size());
        for(Map.Entry<String, NDArray> entry: this.parameterArrays.entrySet()){
            dos.writeUTF(entry.getKey());
            dos.write(entry.getValue().encode());
        }
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException, MalformedModelException, NoSuchFieldException, IllegalAccessException {
        DataInputStream dis = new DataInputStream(ois);
        this.manager = NDManager.newBaseManager();
        this.parameterArrays = new ConcurrentHashMap<>();
        this.gradientArrays = new HashMap<>();
        int i = dis.readInt();
        for(;i>0;i--){
          String id = dis.readUTF();
          NDArray value = this.manager.decode(dis);
          value.setRequiresGradient(true);
          this.parameterArrays.putIfAbsent(id, value);
          this.gradientArrays.putIfAbsent(id, value.zerosLike());
        }

    }
}
