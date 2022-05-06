package functions.nn;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.training.ParameterStore;
import ai.djl.util.Pair;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyParameterStore extends ParameterStore implements Serializable {
    public Map<String, NDArray> parameterArrays;
    public Map<String, Tuple2<NDArray, Integer>> gradientArrays;
    private transient NDManager manager;

    public MyParameterStore() {
        this(NDManager.newBaseManager());
    }

    public MyParameterStore(NDManager manager) {
        this.manager = manager;
        this.parameterArrays = new ConcurrentHashMap<>();
        this.gradientArrays = new HashMap<>();
    }

    public static boolean isTensorCorrect(NDArray arr) {
        return arr.any().getBoolean() && !arr.isNaN().any().getBoolean() && !arr.isInfinite().any().getBoolean();
    }

    public void setNDManager(NDManager newManager) {
        this.parameterArrays.forEach((id, value) -> {
            value.attach(newManager);
        });
        this.gradientArrays.forEach((id, value) -> {
            value._1.attach(newManager);
        });
        manager.close();
        this.manager = newManager;
    }

    /**
     * Step and update model parameters from the acquired gradients
     */
    public void step() {
        parameterArrays.forEach((key, item) -> {
            item.setRequiresGradient(false);
            NDArray grad = gradientArrays.get(key)._1;
            item.addi(grad);
            System.out.println(item);
        });
    }

    /**
     * Replace The Parameters
     *
     * @param newParams
     */
    public void updateParameters(Map<String, NDArray> newParams) {
        newParams.forEach((key, item) -> {
            if (parameterArrays.containsKey(key)) {
                NDArray currentParam = parameterArrays.get(key);
                if (currentParam != item) currentParam.close();
            }
            item.attach(getManager());
            parameterArrays.put(key, item);
        });
    }

    /**
     * Change ids of model parameters to a single standard
     *
     * @param model
     */
    public void canonizeModel(Model model) {
        String modelName = model.getName();
        ParameterList params = model.getBlock().getParameters();
        try {
            Field idField = Parameter.class.getDeclaredField("id");
            idField.setAccessible(true);
            for (Pair<String, Parameter> param : params) {
                idField.set(param.getValue(), modelName + param.getKey() + param.getValue().getName());
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Load the model parameters to memory
     *
     * @param model
     */
    public void loadModel(Model model) {
        model.getBlock().getParameters().forEach(item -> {
            this.parameterArrays.putIfAbsent(item.getValue().getId(), item.getValue().getArray());
            this.gradientArrays.putIfAbsent(item.getValue().getId(), new Tuple2(item.getValue().getArray().zerosLike(), 0));
        });
    }

    /**
     * Restore the model parameters from the parameters in this Store
     *
     * @param model
     */
    public void restoreModel(Model model) {
        model.getBlock().getParameters().forEach(item -> {
            NDArray thisArray = this.parameterArrays.get(item.getValue().getId());
            item.getValue().close();
            item.getValue().setShape(null);
            item.getValue().setArray(thisArray);
        });
    }

    /**
     * Remote Gradient Collection is always Meaned over
     *
     * @param grad
     * @param parameterId
     */
    public void meanAccumulateGrads(Tuple2<NDArray, Integer> grad, String parameterId) {
        if (grad._2 <= 0 || !isTensorCorrect(grad._1)) return;
        NDManager tmpManager = getManager().newSubManager();
        Tuple2<NDArray, Integer> thisGrad = gradientArrays.get(parameterId);
        thisGrad._1.attach(tmpManager);
        grad._1.attach(tmpManager);
        int sumTotal = thisGrad._2 + grad._2;
        NDArray thisTotal = thisGrad._1.mul(thisGrad._2);
        NDArray remoteTotal = grad._1.mul(grad._2);
        NDArray finalResult = (thisTotal.add(remoteTotal)).div(sumTotal);
        finalResult.attach(getManager());
        gradientArrays.put(parameterId, new Tuple2<>(finalResult, sumTotal));
        tmpManager.close();
    }

    /**
     * Local Gradients are summed always
     *
     * @param grad
     * @param parameterId
     */
    public void sumAccumulateGrads(Tuple2<NDArray, Integer> grad, String parameterId) {
        if (grad._2 <= 0 || !isTensorCorrect(grad._1)) return;
        Tuple2<NDArray, Integer> currentGrad = gradientArrays.get(parameterId);
        currentGrad._1.addi(grad._1);
        gradientArrays.put(parameterId, new Tuple2<>(currentGrad._1, currentGrad._2 + grad._2));
    }

    /**
     * Add Collected Gradients to this guy
     *
     * @param newGrads
     */
    public void meanAccumulateGrads(Map<String, Tuple2<NDArray, Integer>> newGrads) {
        newGrads.forEach((key, items) -> {
            meanAccumulateGrads(items, key);
        });
    }

    /**
     * Nullify gradients
     */
    public void resetGrads() {
        this.gradientArrays.forEach((key, item) -> {
            item._1.setRequiresGradient(false);
            gradientArrays.put(key, new Tuple2<>(item._1.zerosLike(), 0));
            item._1.close();

        });
    }

    @Override
    public NDArray getValue(Parameter parameter, Device device, boolean training) {
        NDArray valueParam = this.parameterArrays.get(parameter.getId());
        if (valueParam.hasGradient() && !training) {
            NDArray grad = valueParam.getGradient();
            meanAccumulateGrads(new Tuple2<>(grad, 1), parameter.getId());
            valueParam.setRequiresGradient(false);
        } else if (!valueParam.hasGradient() && training) {
            valueParam.setRequiresGradient(true);
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
        for (Map.Entry<String, NDArray> entry : this.parameterArrays.entrySet()) {
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
        for (; i > 0; i--) {
            String id = dis.readUTF();
            NDArray value = this.manager.decode(dis);
            this.parameterArrays.putIfAbsent(id, value);
            this.gradientArrays.putIfAbsent(id, new Tuple2(value.zerosLike(), 0));
        }

    }
}
