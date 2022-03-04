package serializers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;

import java.io.IOException;

public class TensorSerializer extends Serializer<NDArray> {
    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.writeBytes(o.encode());
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        try {
            NDArray tmp =  NDManager.newBaseManager().decode(input.getInputStream());
            return tmp;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.toDevice(original.getDevice(), true);
    }
}
