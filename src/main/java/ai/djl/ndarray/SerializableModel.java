package ai.djl.ndarray;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.serializers.ParameterSerializer;
import ai.djl.serializers.TensorSerializer;
import ai.djl.training.Trainer;
import ai.djl.training.TrainingConfig;
import ai.djl.translate.Translator;
import ai.djl.util.Pair;
import ai.djl.util.PairList;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import javax.annotation.Nonnull;
import java.io.*;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;


/**
 * A model that contains a single block and that is able to serialize itself
 * Serialization of this model is actually falled back by creating a kryo serializer instead
 *
 * @param <T> the class of the upper block contained in this model
 */
public class SerializableModel<T extends Block> implements Serializable, Model {
    public T block = null;
    public String modelName = null;
    public transient PairList<String, Shape> inputData = null;
    protected transient NDManager manager;

    public SerializableModel() {

    }

    public SerializableModel(String modelName, Block block) {
        this.block = (T) block;
        this.modelName = modelName;
        canonizeModel();
    }

    /**
     * Set the manager of this model + move all the parameters to this manager
     *
     * @param manager NDManager
     */
    public void setManager(@Nonnull NDManager manager) {
        this.manager = manager;
        getBlock().getParameters().forEach(item -> {
            if (item.getValue().isInitialized()) {
                item.getValue().getArray().detach();
                item.getValue().getArray().attach(manager);
            }
        });
    }

    @Override
    public void load(Path modelPath, String prefix, Map<String, ?> options) throws IOException, MalformedModelException {
        File folder = new File(String.valueOf(modelPath));
        FilenameFilter onlyNumpy = (dir, name) -> name.toLowerCase().endsWith(".npy");
        List<File> numpyParameterFiles = new ArrayList<>();
        Collections.addAll(numpyParameterFiles, folder.listFiles(onlyNumpy));
        numpyParameterFiles.sort(Comparator.comparing(File::toString));
        getBlock().getParameters().forEach(param -> {
            try {
                System.out.println(numpyParameterFiles.get(0));
                InputStream in = new FileInputStream(numpyParameterFiles.remove(0));
                NDArray tmp = NDSerializer.decodeNumpy(getNDManager(), in);
                param.getValue().setArray(tmp);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public void load(InputStream is, Map<String, ?> options) throws IOException, MalformedModelException {

    }

    @Override
    public void save(Path modelPath, String newModelName) throws IOException {

    }

    @Override
    public Path getModelPath() {
        return null;
    }

    @Override
    public T getBlock() {
        return block;
    }

    @Override
    public void setBlock(Block block) {
        this.block = (T) block;
    }

    @Override
    public String getName() {
        return modelName;
    }

    @Override
    public String getProperty(String key) {
        return null;
    }

    @Override
    public void setProperty(String key, String value) {

    }

    @Override
    public NDManager getNDManager() {
        return manager;
    }

    @Override
    public Trainer newTrainer(TrainingConfig trainingConfig) {
        return null;
    }

    @Override
    public <I, O> Predictor<I, O> newPredictor(Translator<I, O> translator, Device device) {
        return null;
    }

    @Override
    public PairList<String, Shape> describeInput() {
        if (inputData == null) {
            inputData = block.describeInput();
        }
        return inputData;
    }

    @Override
    public PairList<String, Shape> describeOutput() {
        return null;
    }

    @Override
    public String[] getArtifactNames() {
        return new String[0];
    }

    @Override
    public <T> T getArtifact(String name, Function<InputStream, T> function) throws IOException {
        return null;
    }

    @Override
    public URL getArtifact(String name) throws IOException {
        return null;
    }

    @Override
    public InputStream getArtifactAsStream(String name) throws IOException {
        return null;
    }

    @Override
    public DataType getDataType() {
        return null;
    }

    @Override
    public void setDataType(DataType dataType) {

    }

    @Override
    public void close() {
        getBlock().getParameters().forEach(item -> {
            item.getValue().close();
        });
    }

    @Override
    public void load(Path modelPath) throws IOException, MalformedModelException {
        Model.super.load(modelPath);
    }

    @Override
    public void load(Path modelPath, String prefix) throws IOException, MalformedModelException {
        Model.super.load(modelPath, prefix);
    }

    @Override
    public void load(InputStream is) throws IOException, MalformedModelException {
        Model.super.load(is);
    }

    @Override
    public <I, O> Predictor<I, O> newPredictor(Translator<I, O> translator) {
        return Model.super.newPredictor(translator);
    }

    @Override
    public void cast(DataType dataType) {
        Model.super.cast(dataType);
    }

    @Override
    public void quantize() {
        Model.super.quantize();
    }


    /**
     * Remove the randoom ids from the parameters of this block
     *
     * @implNote Do this before sending the model to the operator
     */
    public SerializableModel<T> canonizeModel() {
        String modelName = getName();
        ParameterList params = getBlock().getParameters();
        try {
            Field idField = Parameter.class.getDeclaredField("id");
            idField.setAccessible(true);
            int i = 0;
            for (Pair<String, Parameter> param : params) {
                idField.set(param.getValue(), String.format("{%s}%s:%s", ++i, modelName, param.getKey()));
            }
            idField.setAccessible(false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Register all classes possible in DGL otherwise error is throws
     */
    private static void registerAllClasses(Kryo a){
        a.setClassLoader(Thread.currentThread().getContextClassLoader());
        ((Kryo.DefaultInstantiatorStrategy) a.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        a.register(PtNDArray.class, new TensorSerializer());
        a.register(JavaTensor.class, new TensorSerializer());
        a.register(Parameter.class, new ParameterSerializer());
    }

    /**
     * Fallback to a Kryo Serializer
     */
    private void writeObject(ObjectOutputStream oos) throws IOException {
        Kryo a = new Kryo();
        registerAllClasses(a);
        Output output = new Output(oos);
        a.writeObject(output, this);
        output.flush();
    }

    /**
     * Fallback to a KryoSerializer
     */
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException, MalformedModelException, NoSuchFieldException, IllegalAccessException {
        Kryo a = new Kryo();
        registerAllClasses(a);
        Input input = new Input(ois);
        SerializableModel<T> tmp = a.readObject(input, SerializableModel.class);
        this.block = tmp.block;
        this.modelName = tmp.modelName;
    }

}
