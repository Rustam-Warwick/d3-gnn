//package functions.nn;
//
//import ai.ai.ai.ai.ai.ai.ai.djl.Device;
//import ai.ai.ai.ai.ai.ai.ai.djl.Model;
//import ai.ai.ai.ai.ai.ai.ai.djl.ndarray.NDArray;
//import ai.ai.ai.ai.ai.ai.ai.djl.ndarray.NDManager;
//import ai.ai.ai.ai.ai.ai.ai.djl.nn.Parameter;
//import ai.ai.ai.ai.ai.ai.ai.djl.nn.ParameterList;
//import ai.ai.ai.ai.ai.ai.djl.training.ParameterStore;
//import ai.ai.ai.ai.ai.ai.ai.djl.util.Pair;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//import java.io.Serializable;
//import java.lang.reflect.Field;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class MyParameterStore extends ParameterStore implements Serializable {
//    public Map<String, NDArray> parameterArrays;
//    public Map<String, Tuple2<NDArray, Integer>> gradientArrays;
//    private transient NDManager manager;
//
//    public MyParameterStore() {
//        this(NDManager.newBaseManager());
//    }
//
//    public MyParameterStore(NDManager manager) {
//        this.manager = manager;
//        this.parameterArrays = new ConcurrentHashMap<>();
//        this.gradientArrays = new HashMap<>();
//    }
//
//    public static boolean isTensorCorrect(NDArray arr) {
//        return arr.any().getBoolean() && !arr.isNaN().any().getBoolean() && !arr.isInfinite().any().getBoolean();
//    }
//
//    public void setNDManager(NDManager newManager) {
//        this.parameterArrays.forEach((id, value) -> {
//            value.attach(newManager);
//        });
//        this.gradientArrays.forEach((id, value) -> {
//            value.f0.attach(newManager);
//        });
//        this.manager = newManager;
//    }
//
//    /**
//     * Step and update model parameters from the acquired gradients
//     */
//    public void step() {
//        parameterArrays.forEach((key, item) -> {
//            item.setRequiresGradient(false);
//            NDArray grad = gradientArrays.get(key).f0;
//            item.addi(grad);
//            System.out.println(item);
//        });
//    }
//
//    /**
//     * Replace The Parameters
//     *
//     * @param newParams
//     */
//    public void updateParameters(Map<String, NDArray> newParams) {
//        newParams.forEach((key, item) -> {
//            if (parameterArrays.containsKey(key)) {
//                NDArray currentParam = parameterArrays.get(key);
//            }
//            item.attach(getManager());
//            parameterArrays.put(key, item);
//        });
//    }
//
//    @Deprecated
//    public void canonizeModel(Model model) {
//        String modelName = model.getName();
//        ParameterList params = model.getBlock().getParameters();
//        try {
//            Field idField = Parameter.class.getDeclaredField("id");
//            idField.setAccessible(true);
//            for (Pair<String, Parameter> param : params) {
//                idField.set(param.getValue(), modelName + param.getKey() + param.getValue().getName());
//            }
//        } catch (NoSuchFieldException | IllegalAccessException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Load the model parameters to memory
//     *
//     * @param model model to be loaded
//     * @implNote Stores the model parameters and gradients
//     */
//    public void loadModel(Model model) {
//        model.getBlock().getParameters().forEach(item -> {
//            this.parameterArrays.putIfAbsent(item.getValue().getId(), item.getValue().getArray());
//            this.gradientArrays.putIfAbsent(item.getValue().getId(), new Tuple2<NDArray, Integer>(item.getValue().getArray().zerosLike(), 0));
//        });
//    }
//
//    /**
//     * Restore the model parameters from the parameters in this Store
//     *
//     * @param model
//     */
//    public void restoreModel(Model model) {
//        model.getBlock().getParameters().forEach(item -> {
//            NDArray thisArray = this.parameterArrays.get(item.getValue().getId());
//            item.getValue().setShape(null);
//            item.getValue().setArray(thisArray);
//        });
//    }
//
//    /**
//     * Remote Gradient Collection is always Meaned over
//     *
//     * @param grad
//     * @param parameterId
//     */
//    public void meanAccumulateGrads(Tuple2<NDArray, Integer> grad, String parameterId) {
//        if (grad.f1 <= 0 || !isTensorCorrect(grad.f0)) return;
//        NDManager tmpManager = getManager().newSubManager();
//        Tuple2<NDArray, Integer> thisGrad = gradientArrays.get(parameterId);
//        thisGrad.f0.attach(tmpManager);
//        grad.f0.attach(tmpManager);
//        int sumTotal = thisGrad.f1 + grad.f1;
//        NDArray thisTotal = thisGrad.f0.mul(thisGrad.f1);
//        NDArray remoteTotal = grad.f0.mul(grad.f1);
//        NDArray finalResult = (thisTotal.add(remoteTotal)).div(sumTotal);
//        finalResult.attach(getManager());
//        gradientArrays.put(parameterId, new Tuple2<>(finalResult, sumTotal));
//    }
//
//    /**
//     * Local Gradients are summed always
//     *
//     * @param grad
//     * @param parameterId
//     */
//    public void sumAccumulateGrads(Tuple2<NDArray, Integer> grad, String parameterId) {
//        if (grad.f1 <= 0 || !isTensorCorrect(grad.f0)) return;
//        Tuple2<NDArray, Integer> currentGrad = gradientArrays.get(parameterId);
//        currentGrad.f0.addi(grad.f0);
//        gradientArrays.put(parameterId, new Tuple2<>(currentGrad.f0, currentGrad.f1 + grad.f1));
//    }
//
//    /**
//     * Add Collected Gradients to this guy
//     *
//     * @param newGrads
//     */
//    public void meanAccumulateGrads(Map<String, Tuple2<NDArray, Integer>> newGrads) {
//        newGrads.forEach((key, items) -> {
//            meanAccumulateGrads(items, key);
//        });
//    }
//
//    /**
//     * Nullify gradients
//     */
//    public void resetGrads() {
//        this.gradientArrays.forEach((key, item) -> {
//            item.f0.setRequiresGradient(false);
//            gradientArrays.put(key, new Tuple2<>(item.f0.zerosLike(), 0));
//
//        });
//    }
//
//    @Override
//    public NDArray getValue(Parameter parameter, Device device, boolean training) {
//        if (parameter != null && parameterArrays.containsKey(parameter.getId())) {
//            NDArray valueParam = this.parameterArrays.get(parameter.getId());
//            if (valueParam.hasGradient() && !training) {
//                NDArray grad = valueParam.getGradient();
//                meanAccumulateGrads(new Tuple2<>(grad, 1), parameter.getId());
//                valueParam.setRequiresGradient(false);
//            } else if (!valueParam.hasGradient() && training) {
//                valueParam.setRequiresGradient(true);
//            }
//
//            return valueParam;
//        } else {
//            return null;
//        }
//
//    }
//
//    @Override
//    public NDManager getManager() {
//        return super.getManager();
//    }
//
//    @Override
//    public void sync() {
//        super.sync();
//    }
//}
