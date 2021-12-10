package serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.nd4j.nativeblas.Nd4jCpu;
import org.tensorflow.EagerSession;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.op.Scope;
import org.tensorflow.op.core.Constant;
import org.tensorflow.op.io.SerializeTensor;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.family.TType;

public class TensorSerializer extends Serializer<Tensor> {

    @Override
    public void write(Kryo kryo, Output output, Tensor object) {
        System.out.println("SAla,m");
    }

    @Override
    public Tensor read(Kryo kryo, Input input, Class<Tensor> type) {
        System.out.println("SLAM");
        return null;
    }
}
