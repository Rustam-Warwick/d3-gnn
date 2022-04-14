package helpers;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;


/**
 * Proxy wrapper for NDArray. NDArray that need to be managed by JVM should be wrapped by this
 * Note that all operations just return a normal NDArray
 * This is done because some tensors are intrinsic to the system and shouldn't be garbadge collected at all, like backward pass gradients and etc.
 */
public class JavaTensor extends PtNDArray {
    private JavaTensor(PtNDManager manager, long handle) {
        super(manager, handle);
    }

    private JavaTensor(PtNDManager manager, long handle, ByteBuffer data) {
        super(manager, handle, data);
    }

    public JavaTensor(NDArray arr) {
        this(((PtNDArray) arr).getManager(), ((PtNDArray) arr).getHandle(), arr.toByteBuffer());
        arr.detach();
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
    }


}
