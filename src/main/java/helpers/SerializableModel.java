package helpers;

import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.Trainer;
import ai.djl.training.TrainingConfig;
import ai.djl.translate.Translator;
import ai.djl.util.Pair;
import ai.djl.util.PairList;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

/**
 * Proxy for a Model that is able to serialize itself
 */
public class SerializableModel implements Model, Serializable, Externalizable {
    public transient Model model;
    public SerializableModel(){
        this.model = null;
    }
    public SerializableModel(Model a){
        this.model = a;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (this.getBlock() == null || !this.getBlock().isInitialized()) {
            throw new IllegalStateException("Model has not be trained or loaded yet.");
        }
            out.writeBytes("DJL@");
            out.writeInt(1);
            out.writeUTF(this.getName());
            out.writeUTF(this.getDataType().name());
            PairList<String, Shape> inputData = this.getBlock().describeInput();
            out.writeInt(inputData.size());
            for (Pair<String, Shape> desc : inputData) {
                String name = desc.getKey();
                if (name == null) {
                    out.writeUTF("");
                } else {
                    out.writeUTF(name);
                }
                out.write(desc.getValue().getEncoded());
            }
            out.writeInt(0);
            DataOutputStream tmp = new DataOutputStream(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                }
            });
            this.getBlock().saveParameters(tmp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        InputStream tmp = new InputStream() {
            @Override
            public int read() throws IOException {
                return in.read();
            }
        };
        try{
            Model model = newInstance("inference");
            model.load(tmp);
            this.model = model;
        }catch (MalformedModelException e){

        }

    }

    public static SerializableModel of(Model a){
        return new SerializableModel(a);
    }

    public static Model newInstance(String name) {
        return Model.newInstance(name);
    }

    public static Model newInstance(String name, Device device) {
        return Model.newInstance(name, device);
    }

    public static Model newInstance(String name, String engineName) {
        return Model.newInstance(name, engineName);
    }

    public static Model newInstance(String name, Device device, String engineName) {
        return Model.newInstance(name, device, engineName);
    }

    @Override
    public void load(Path modelPath) throws IOException, MalformedModelException {
        model.load(modelPath);
    }

    @Override
    public void load(Path modelPath, String prefix) throws IOException, MalformedModelException {
        model.load(modelPath, prefix);
    }

    @Override
    public void load(Path modelPath, String prefix, Map<String, ?> options) throws IOException, MalformedModelException {
        model.load(modelPath, prefix, options);
    }

    @Override
    public void load(InputStream is) throws IOException, MalformedModelException {
        model.load(is);
    }

    @Override
    public void load(InputStream is, Map<String, ?> options) throws IOException, MalformedModelException {
        model.load(is, options);
    }

    @Override
    public void save(Path modelPath, String newModelName) throws IOException {
        model.save(modelPath, newModelName);
    }

    @Override
    public Path getModelPath() {
        return model.getModelPath();
    }

    @Override
    public Block getBlock() {
        return model.getBlock();
    }

    @Override
    public void setBlock(Block block) {
        model.setBlock(block);
    }

    @Override
    public String getName() {
        return model.getName();
    }

    @Override
    public String getProperty(String key) {
        return model.getProperty(key);
    }

    @Override
    public void setProperty(String key, String value) {
        model.setProperty(key, value);
    }

    @Override
    public NDManager getNDManager() {
        return model.getNDManager();
    }

    @Override
    public Trainer newTrainer(TrainingConfig trainingConfig) {
        return model.newTrainer(trainingConfig);
    }

    @Override
    public <I, O> Predictor<I, O> newPredictor(Translator<I, O> translator) {
        return model.newPredictor(translator);
    }

    @Override
    public <I, O> Predictor<I, O> newPredictor(Translator<I, O> translator, Device device) {
        return model.newPredictor(translator, device);
    }

    @Override
    public PairList<String, Shape> describeInput() {
        return model.describeInput();
    }

    @Override
    public PairList<String, Shape> describeOutput() {
        return model.describeOutput();
    }

    @Override
    public String[] getArtifactNames() {
        return model.getArtifactNames();
    }

    @Override
    public <T> T getArtifact(String name, Function<InputStream, T> function) throws IOException {
        return model.getArtifact(name, function);
    }

    @Override
    public URL getArtifact(String name) throws IOException {
        return model.getArtifact(name);
    }

    @Override
    public InputStream getArtifactAsStream(String name) throws IOException {
        return model.getArtifactAsStream(name);
    }

    @Override
    public void setDataType(DataType dataType) {
        model.setDataType(dataType);
    }

    @Override
    public DataType getDataType() {
        return model.getDataType();
    }

    @Override
    public void cast(DataType dataType) {
        model.cast(dataType);
    }

    @Override
    public void quantize() {
        model.quantize();
    }

    @Override
    public void close() {
        model.close();
    }
}
