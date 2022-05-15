package ai.djl.serializers;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

/**
 * Kryo implementation of Tensor Serializer. Works with all Pt
 */
public class NDArraySerializer extends Serializer<NDArray> {

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.write(o.encode());
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        try {
            if(!BaseNDManager.threadNDManager.get().isOpen()){
                System.out.println();
            }
            NDArray array = BaseNDManager.threadNDManager.get().decode(input);
            return array;
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
