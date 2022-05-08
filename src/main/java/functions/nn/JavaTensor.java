package functions.nn;

import ai.djl.ndarray.NDArray;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.engine.PtNDManager;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;


/**
 * Proxy wrapper for NDArray. NDArray that need to be managed by JVM should be wrapped by this
 * In other words if your want NDArray to be GC-ed and closed by JVM this should be the class of choice
 * @implNote All non-inplace tensor operations will still return a normal Tensor so you have to manually manage all tensors
 * This is done because some tensors are intrinsic to the system and shouldn't be garbadge collected at all, like backward pass gradients and etc.
 */
public class JavaTensor extends PtNDArray {
    public static final Cleaner cleaer = Cleaner.create();


    private JavaTensor(PtNDManager manager, long handle) {
        super(manager, handle);
    }

    private JavaTensor(PtNDManager manager, long handle, ByteBuffer data) {
        super(manager, handle, data);
    }

    public JavaTensor(NDArray arr) {
        this(((PtNDArray) arr).getManager(), ((PtNDArray) arr).getHandle(), arr.toByteBuffer());
        this.detach();
        cleaer.register(this, new State((PtNDArray) arr));
    }

    @Override
    public void close() {
        // Do nothing already registered for deletion
    }

    public static class State implements Runnable {
        public PtNDArray resource;

        public State(PtNDArray resource) {
            this.resource = resource;
        }

        @Override
        public void run() {
            if (!resource.isReleased()) {
                resource.close();
            }
        }
    }

}