package ai.djl.serializers;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NDManagerSerializer extends Serializer<NDManager> {

    @Override
    public void write(Kryo kryo, Output output, NDManager object) {
        // Do not write
    }

    @Override
    public NDManager read(Kryo kryo, Input input, Class<NDManager> type) {
        return BaseNDManager.threadNDManager.get();
    }
}
