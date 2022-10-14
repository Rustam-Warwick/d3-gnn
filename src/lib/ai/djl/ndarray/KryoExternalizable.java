package ai.djl.ndarray;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.*;

public interface KryoExternalizable extends Externalizable {

    @Override
    default void writeExternal(ObjectOutput out) {
        try {
            ExecutionConfig config = NDHelper.addSerializers(new ExecutionConfig());
            TypeSerializer serializer = TypeInformation.of(this.getClass()).createSerializer(config);
            OutputStream tmp = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                }
            };
            serializer.serialize(this, new DataOutputViewStreamWrapper(tmp));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not serialize the model");
        }
    }

    @Override
    default void readExternal(ObjectInput in) {
        try {
            ExecutionConfig config = NDHelper.addSerializers(new ExecutionConfig());
            TypeSerializer serializer = TypeInformation.of(getClass()).createSerializer(config);
            InputStream tmp = new InputStream() {
                @Override
                public int read() throws IOException {
                    return in.read();
                }
            };
            Object tmpModel = serializer.deserialize(new DataInputViewStreamWrapper(tmp));
            NDHelper.copyFields(tmpModel, this);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Count not deserialize the model");
        }
    }
}
