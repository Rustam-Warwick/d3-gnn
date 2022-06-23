package ai.djl.util;

import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Gracefully release Native Memory resources if the array was not explicitly closed
 */
public class PtNDArrayFinalizeTask implements Runnable {
    private final AtomicReference<Long> handle;

    public PtNDArrayFinalizeTask(PtNDArray array) {
        handle = array.handle;
    }

    @Override
    public void run() {
        Long pointer = handle.getAndSet(null);
        if (pointer != null) {
            JniUtils.deleteNDArray(pointer);
        }
    }
}
