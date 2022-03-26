package serializers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.PtNDArray;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import helpers.JavaTensor;
import helpers.TensorCleaner;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TensorSerializer extends Serializer<NDArray> {
    private static final String MAGIC_NUMBER = "NDAR";
    private static final Integer VERSION = 3;
    private static final NDManager manager = NDManager.newBaseManager();

    @Override
    public void write(Kryo kryo, Output output, NDArray o) {
        // magic string for version identification
        output.writeString(MAGIC_NUMBER);
        output.writeInt(VERSION);
        String name = o.getName();
        if (name == null) {
            output.write(0);
        } else {
            output.write(1);
            output.writeString(name);
        }
        output.writeString(o.getSparseFormat().name());
        output.writeString(o.getDataType().name());

        Shape shape = o.getShape();
        output.write(shape.getEncoded());

//        ByteBuffer bb = o.toByteBuffer();
//        output.write(bb.order() == ByteOrder.BIG_ENDIAN ? '>' : '<');
//        int length = bb.remaining();
        byte[] bb = o.toByteArray();
        output.writeInt(bb.length);
        output.write(bb);

    }

    @Override
    public NDArray read(Kryo kryo, Input input, Class aClass) {
        if (!"NDAR".equals(input.readString())) {
            throw new IllegalArgumentException("Malformed NDArray data");
        }

        // NDArray encode version
        int version = input.readInt();
        if (version < 1 || version > VERSION) {
            throw new IllegalArgumentException("Unexpected NDArray encode version " + version);
        }

        String name = null;
        if (version > 1) {
            byte flag = input.readByte();
            if (flag == 1) {
                name = input.readString();
            }
        }

        input.readString(); // ignore SparseFormat

        // DataType
        DataType dataType = DataType.valueOf(input.readString());

        // Shape
        Shape shape = null;
        try {
            shape = Shape.decode(new DataInputStream(input));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Data


        int length = input.readInt();
        ByteBuffer data = manager.allocateDirect(length);

        byte[] x = input.readBytes(length);
        data.put(x);
        data.rewind();
        NDArray array = manager.create(dataType.asDataType(data), shape, dataType);
        array.setName(name);
        return new JavaTensor((PtNDArray) array);
    }


    @Override
    public NDArray copy(Kryo kryo, NDArray original) {
        return original.duplicate();
    }


}
