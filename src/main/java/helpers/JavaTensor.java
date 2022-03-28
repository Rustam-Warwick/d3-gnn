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
public class JavaTensor extends PtNDArray{
    public static final Cleaner cleaer = Cleaner.create();


    private JavaTensor(PtNDManager manager, long handle) {
        super(manager, handle);
    }

    private JavaTensor(PtNDManager manager, long handle, ByteBuffer data) {
        super(manager, handle, data);
    }

    public JavaTensor(NDArray arr){
        this(((PtNDArray) arr).getManager(), ((PtNDArray) arr).getHandle(), ((PtNDArray) arr).toByteBuffer());
        this.detach();
        cleaer.register(this, new State(arr));
    }

    public static class State implements Runnable{
        public NDArray resource;
        public State(NDArray resource){
            this.resource = resource;
        }

        @Override
        public void run() {
            resource.close();
        }
    }

}
