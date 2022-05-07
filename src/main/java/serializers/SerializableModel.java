package serializers;

import ai.djl.BaseModel;
import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.types.DataType;
import ai.djl.nn.Block;
import ai.djl.translate.Translator;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;

public class SerializableModel<T extends Block> extends BaseModel implements Serializable {

    public SerializableModel(String modelName) {
        super(modelName);
    }

    @Override
    public void load(Path modelPath, String prefix, Map<String, ?> options) throws IOException, MalformedModelException {

    }

    @Override
    public void load(Path modelPath) throws IOException, MalformedModelException {
        super.load(modelPath);
    }

    @Override
    public void load(Path modelPath, String prefix) throws IOException, MalformedModelException {
        super.load(modelPath, prefix);
    }

    @Override
    public void load(InputStream is) throws IOException, MalformedModelException {
        super.load(is);
    }

    @Override
    public <I, O> Predictor<I, O> newPredictor(Translator<I, O> translator) {
        return super.newPredictor(translator);
    }

    @Override
    public void cast(DataType dataType) {
        super.cast(dataType);
    }

    @Override
    public void quantize() {
        super.quantize();
    }
}
