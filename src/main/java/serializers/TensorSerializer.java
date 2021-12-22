package serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBufferWindow;
import org.tensorflow.ndarray.buffer.DataStorageVisitor;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Constant;
import org.tensorflow.op.io.ParseTensor;
import org.tensorflow.op.io.SerializeTensor;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TString;
import org.tensorflow.types.family.TType;
import types.TFWrapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TensorSerializer extends Serializer<TFWrapper> {
    private Ops ops;
    public TensorSerializer(){
        super();
        this.ops = Ops.create();
    }
    @Override
    public void write(Kryo kryo, Output output, TFWrapper object) {

        MyBufferMapper buffer = new MyBufferMapper(output);
        output.writeString(object.getValue().type().getName());
        SerializeTensor a = this.ops.io.serializeTensor(this.ops.constantOf(object.value));
        a.serialized().asTensor().asBytes().write(buffer);
    }

    @Override
    public TFWrapper read(Kryo kryo, Input input, Class<TFWrapper> type) {
        try {
            String className = input.readString();
            Class clazz = Class.forName(className);
            int length = input.readInt();
            byte[] rawTensorData = input.readBytes(length);
            String decodedRawTensor = new String(rawTensorData,StandardCharsets.US_ASCII);
            Constant<TString> str = this.ops.constant(StandardCharsets.US_ASCII,decodedRawTensor);
            TType a = ops.io.parseTensor(str,clazz).asTensor();
            return new TFWrapper(a);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }

    }

    public class MyBufferMapper implements DataBuffer<byte[]>{
        public Output kryo;
        public MyBufferMapper(Output kryo){
            this.kryo = kryo;
        }
        @Override
        public long size() {
            return 1024;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public byte[] getObject(long index) {
            return new byte[0];
        }

        @Override
        public DataBuffer<byte[]> setObject(byte[] value, long index) {
            return null;
        }

        @Override
        public DataBuffer<byte[]> read(byte[][] dst, int offset, int length) {
            return null;
        }

        @Override
        public DataBuffer<byte[]> write(byte[][] src, int offset, int length) {
            return null;
        }

        @Override
        public DataBuffer<byte[]> copyTo(DataBuffer<byte[]> dst, long size) {
            byte[][] a = new byte[1][];
            dst.read(a);
            kryo.writeInt(a[0].length);
            kryo.writeBytes(a[0]);
            return this;
        }

        @Override
        public DataBuffer<byte[]> slice(long index, long size) {
            return null;
        }

        @Override
        public DataBuffer<byte[]> read(byte[][] dst) {
            return DataBuffer.super.read(dst);
        }

        @Override
        public DataBuffer<byte[]> write(byte[][] src) {
            return DataBuffer.super.write(src);
        }

        @Override
        public DataBuffer<byte[]> offset(long index) {
            return DataBuffer.super.offset(index);
        }

        @Override
        public DataBuffer<byte[]> narrow(long size) {
            return DataBuffer.super.narrow(size);
        }

        @Override
        public DataBufferWindow<? extends DataBuffer<byte[]>> window(long size) {
            return DataBuffer.super.window(size);
        }

        @Override
        public <R> R accept(DataStorageVisitor<R> visitor) {
            return DataBuffer.super.accept(visitor);
        }
    }
}
