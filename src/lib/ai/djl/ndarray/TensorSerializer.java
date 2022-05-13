package ai.djl.ndarray;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

/**
 * Kryo implementation of Tensor Serializer. Works with all Pt
 */
public class TensorSerializer extends Serializer<NDArray> {
    private final static NDManager manager = NDManager.newBaseManager();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.write(o.encode());
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        try {
            NDArray array = manager.decode(input);
            return new JavaTensor(array);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }


}
