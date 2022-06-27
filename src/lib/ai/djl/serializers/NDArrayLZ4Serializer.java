package ai.djl.serializers;

import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;

/**
 * Kryo implementation of Tensor Serializer with LZ4 Compression. Works with all NDArrays
 */
public class NDArrayLZ4Serializer extends Serializer<NDArray> {
    private static final transient DataType[] dataTypes = DataType.values();
    private static final transient LZ4Compressor lz4Compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static final transient LZ4SafeDecompressor lz4DeCompressor = LZ4Factory.fastestInstance().safeDecompressor();
    private static final transient ThreadLocal<ByteBuffer> reuse = ThreadLocal.withInitial(() -> ByteBuffer.allocate(0));

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.writeByte(o.getDataType().ordinal()); // Data Types
        output.writeByte(o.getShape().getShape().length); // Shape length
        output.writeLongs(o.getShape().getShape(), true); // Actual Shapes
        ByteBuffer bb = o.toByteBuffer();
        int maxCompressionLength = lz4Compressor.maxCompressedLength(bb.capacity());
        increaseBufferIfNeeded(maxCompressionLength);
        ByteBuffer thisReuse = reuse.get();
        thisReuse.position(0);
        thisReuse.limit(thisReuse.capacity());
        lz4Compressor.compress(bb.rewind(), thisReuse);
        int actualSize = thisReuse.position();
        thisReuse.position(0);
        thisReuse.limit(actualSize);
        output.writeInts(new int[]{bb.capacity(), actualSize}, true); // Raw Len, Actual Len
        while (thisReuse.hasRemaining()) {
            output.write(thisReuse.get());
        }
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        DataType dataType = dataTypes[input.readByte()]; // Data Type
        long[] shapes = input.readLongs(input.readByte(), true);
        Shape shape = new Shape(shapes); // Shape
        int[] lens = input.readInts(2, true);
        increaseBufferIfNeeded(lens[0]);
        ByteBuffer thisReuse = reuse.get();
        thisReuse.position(0);
        thisReuse.limit(lens[0]);
        byte[] compressedData = input.readBytes(lens[1]);
        lz4DeCompressor.decompress(ByteBuffer.wrap(compressedData), 0, compressedData.length, thisReuse, 0, lens[0]);
        thisReuse.position(0);
        NDArray tmp = LifeCycleNDManager.getInstance().create(shape,dataType);
        tmp.set(thisReuse);
        return LifeCycleNDManager.getInstance().create(thisReuse, shape, dataType);
    }

    private void increaseBufferIfNeeded(int capacity) {
        if (capacity > reuse.get().capacity()) {
            reuse.set(ByteBuffer.allocate(capacity));
        }
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }

}
