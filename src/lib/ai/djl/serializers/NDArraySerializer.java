package ai.djl.serializers;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

/**
 * Kryo implementation of Tensor Serializer. Works with all Pt
 */
public class NDArraySerializer extends Serializer<NDArray> {
    private static transient DataType[] dataTypes = DataType.values();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.writeByte(o.getDataType().ordinal());
        output.writeByte(o.getShape().getShape().length);
        output.writeLongs(o.getShape().getShape());
        ByteBuffer bb = o.toByteBuffer();
        output.writeInt(bb.remaining());
        output.write(bb.array());
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        DataType dataType = dataTypes[input.readByte()];
        long[] shapes = input.readLongs(input.readByte());
        Shape shape = new Shape(shapes);
        int bufferSize = input.readInt();
        ByteBuffer data = BaseNDManager.threadNDManager.get().allocateDirect(bufferSize);
        data.put(input.readBytes(data.capacity()));
        return BaseNDManager.threadNDManager.get().create(data.rewind(),shape, dataType);
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }


}
