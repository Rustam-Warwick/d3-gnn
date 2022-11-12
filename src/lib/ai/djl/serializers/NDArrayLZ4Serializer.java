package ai.djl.serializers;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
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
    private static final DataType[] dataTypes = DataType.values();
    private static final LZ4Compressor lz4Compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static final LZ4SafeDecompressor lz4DeCompressor = LZ4Factory.fastestInstance().safeDecompressor();
    private static final ThreadLocal<ByteBuffer> reuse = ThreadLocal.withInitial(() -> ByteBuffer.allocate(0));
    private transient Boolean isStorage; // If we are serializing to a storage backend

    private transient NDManager manager = BaseNDManager.getManager();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        if (isStorage == null) setIsStorage();
        ByteBuffer bb = o.toByteBuffer();
        output.writeByte(o.getDataType().ordinal()); // Data Types
        output.writeByte(o.getShape().getShape().length); // Shape length
        output.writeLongs(o.getShape().getShape(), true); // Actual Shapes
        int maxCompressionLength = lz4Compressor.maxCompressedLength(bb.capacity());
        increaseBufferIfNeeded(maxCompressionLength);
        ByteBuffer thisReuse = reuse.get();
        thisReuse.position(0);
        thisReuse.limit(thisReuse.capacity());
        lz4Compressor.compress(bb.rewind(), thisReuse);
        bb.rewind();
        int actualSize = thisReuse.position();
        thisReuse.position(0);
        thisReuse.limit(actualSize);
        output.writeInts(new int[]{bb.capacity(), actualSize}, true); // Raw Len, Actual Len
        while (thisReuse.hasRemaining()) {
            output.writeByte(thisReuse.get());
        }
        if (isStorage) o.resume();
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        if (isStorage == null) setIsStorage();
        int ordinal = input.readByte();
        DataType dataType = dataTypes[ordinal]; // Data Type
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
        return manager.create(thisReuse, shape, dataType);
    }

    private void increaseBufferIfNeeded(int capacity) {
        if (capacity > reuse.get().capacity()) {
            reuse.set(ByteBuffer.allocate(capacity));
        }
    }

    private void setIsStorage() {
        isStorage = false;
        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
            if (stackTraceElement.getClassName().contains("UserFacingMapState")) {
                isStorage = true;
                return;
            }
        }
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }

}
