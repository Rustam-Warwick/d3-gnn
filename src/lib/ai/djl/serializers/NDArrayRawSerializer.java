package ai.djl.serializers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

/**
 * Kryo implementation of Tensor Serializer. Works with all NDArrays.
 */
public class NDArrayRawSerializer extends Serializer<NDArray> {
    private static final transient DataType[] dataTypes = DataType.values();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        ByteBuffer bb = o.toByteBuffer();
        output.writeByte(o.getDataType().ordinal()); // Data Types
        output.writeByte(o.getShape().getShape().length); // Shape length
        output.writeLongs(o.getShape().getShape(), true); // Actual Shapes
        output.writeInt(bb.capacity());
        output.write(bb.array()); // Actual Data
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        int ordinal = input.readByte();
        if (ordinal == -1) throw new KryoException("This NDARray was closed");
        DataType dataType = dataTypes[ordinal]; // Data Type
        long[] shapes = input.readLongs(input.readByte(), true);
        Shape shape = new Shape(shapes); // Shape
        int bufferSize = input.readInt();
        ByteBuffer data = LifeCycleNDManager.getInstance().allocateDirect(bufferSize);
        data.put(input.readBytes(data.capacity()));
        return LifeCycleNDManager.getInstance().create(data.rewind(), shape, dataType);
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }

}
