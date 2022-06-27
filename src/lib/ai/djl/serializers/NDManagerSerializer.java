package ai.djl.serializers;

import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.ndarray.NDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo Serializer for NDManagers. Send nothing, retrieve the local BaseNDManager
 */
public class NDManagerSerializer extends Serializer<NDManager> {

    @Override
    public void write(Kryo kryo, Output output, NDManager object) {
        // Do not write
    }

    @Override
    public NDManager read(Kryo kryo, Input input, Class<NDManager> type) {
        return LifeCycleNDManager.getInstance();
    }
}