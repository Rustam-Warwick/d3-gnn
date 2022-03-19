package serializers;

import ai.djl.Model;
import ai.djl.ndarray.NDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;

public class ModelSerializer extends JavaSerializer<Model> {
    private static final NDManager manager = NDManager.newBaseManager();
    @Override
    public void write(Kryo kryo, Output output, Model o) {
     }

    @Override
    public Model read(Kryo kryo, Input input, Class aClass) {
        return null;
    }
}
