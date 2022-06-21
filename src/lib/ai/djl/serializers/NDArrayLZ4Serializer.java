package ai.djl.serializers;

import ai.djl.ndarray.BaseNDManager;
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
 * Kryo implementation of Tensor Serializer. Works with all Pt
 */
public class NDArrayLZ4Serializer extends Serializer<NDArray> {
    private static final transient DataType[] dataTypes = DataType.values();
    private static final transient LZ4Compressor lz4Compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static final transient LZ4SafeDecompressor lz4DeCompressor = LZ4Factory.fastestInstance().safeDecompressor();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        output.writeByte(o.getDataType().ordinal()); // Data Types
        output.writeByte(o.getShape().getShape().length); // Shape length
        output.writeLongs(o.getShape().getShape(), true); // Actual Shapes

        ByteBuffer bb = o.toByteBuffer();
        byte[] rawData = bb.array();
        int maxCompressionLength = lz4Compressor.maxCompressedLength(rawData.length);
        byte[] compressedData = new byte[maxCompressionLength];
        int actualSize = lz4Compressor.compress(rawData, compressedData);
        output.writeInts(new int[]{rawData.length, actualSize}, true); // Raw Len, Actual Len
        output.writeBytes(compressedData, 0, actualSize);
    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        DataType dataType = dataTypes[input.readByte()]; // Data Type
        long[] shapes = input.readLongs(input.readByte(), true);
        Shape shape = new Shape(shapes); // Shape
        int[] lens = input.readInts(2, true);
        byte[] actualData = new byte[lens[0]];
        byte[] compressedData = input.readBytes(lens[1]);
        lz4DeCompressor.decompress(compressedData, actualData);
        return BaseNDManager.threadNDManager.get().create(ByteBuffer.wrap(actualData), shape, dataType);
    }

    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }

}
