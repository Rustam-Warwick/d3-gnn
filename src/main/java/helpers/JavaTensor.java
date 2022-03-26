package helpers;

import ai.djl.Device;
import ai.djl.ndarray.BytesSupplier;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.index.NDIndex;
import ai.djl.ndarray.internal.NDArrayEx;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.ndarray.types.SparseFormat;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;
import org.codehaus.janino.Java;


import java.lang.ref.Cleaner;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.function.Function;


/**
 * Proxy wrapper for NDArray. NDArray that need to be managed by JVM should be wrapped by this
 * Note that all operations just return a normal NDArray
 * This is done because some tensors are intrinsic to the system and shouldn't be garbadge collected at all, like backward pass gradients and etc.
 */
public class JavaTensor extends PtNDArray{
    public JavaTensor(PtNDManager manager, long handle) {
        super(manager, handle);
    }

    public JavaTensor(PtNDManager manager, long handle, ByteBuffer data) {
        super(manager, handle, data);
    }

    public JavaTensor(PtNDArray arr){
        this(arr.getManager(), arr.getHandle(), arr.toByteBuffer());
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        System.out.println("Salam");
        this.close();

    }
}
