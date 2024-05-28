/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package ai.djl.ndarray;

import ai.djl.Device;
import ai.djl.engine.Engine;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.util.PairList;
import ai.djl.util.RandomUtils;
import com.github.benmanes.caffeine.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.nio.Buffer;
import java.nio.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@code BaseNDManager} is the default implementation of {@link NDManager}.
 * <strong>
 * Fully changed code such that all registered NDArrays are cleared in a sliding window
 * NDarrays can call {@link LifeCycleControl} methods to delay or resume them from being cleared up
 * </strong>
 *
 * <strong>
 * To aggregate a new engine support we need to create NDManager and place it in ThreadLocal
 * Further retrievals should also be done through threadlocal as well
 * </strong>
 *
 * @author rustambaku13
 * <p>
 * Changes to cache based storage with ticker eviction
 * </p>
 */
public abstract class BaseNDManager implements NDManager {

    protected static final Logger logger = LoggerFactory.getLogger(BaseNDManager.class);

    protected static final Cleaner cleaner = Cleaner.create();

    protected static final Engine engineDefault = Engine.getInstance();

    protected static final ThreadLocal<BaseNDManager> ND_MANAGER_THREAD_LOCAL = new ThreadLocal<>();

    protected final BaseNDManager.ManualTicker ticker = new BaseNDManager.ManualTicker(); // Logical timer depending on the data-rate

    protected final Cache<AutoCloseable, Object> attached = Caffeine.newBuilder()
            .evictionListener((RemovalListener<AutoCloseable, Object>) (key, value, cause) -> {
                try {
                    if (cause.wasEvicted()) {
                        ((LifeCycleControl) key).destroy();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }).expireAfterWrite(3, TimeUnit.NANOSECONDS)
            .ticker(ticker)
            .scheduler(Scheduler.systemScheduler())
            .build();

    protected NDManager parent;

    protected String uid;

    protected String name;

    protected Device device;

    protected byte delayed = 0; // Count of this NDManager getting delayed. Ticker is not incrementing once in delayed state

    protected int scopedCount = 0; // Count of opened tensors when we are in a delayed state

    protected BaseNDManager(NDManager parent, Device device) {
        this.parent = parent;
        this.device = device == null ? defaultDevice() : device;
        uid = Thread.currentThread().getName() + UUID.randomUUID();
        cleaner.register(this, new NDManagerFinalizeTask(this));
    }

    // -------------------------- MY METHODS ---------------------------

    /**
     * Get NDManager from the default engine
     */
    public static NDManager getManager() {
        return engineDefault.newBaseManager(null);
    }

    /**
     * Checks if the input buffer size is match expected data type.
     *
     * @param buffer   the input buffer
     * @param dataType the desired {@code DataType}
     * @param expected the expected size
     * @throws IllegalArgumentException if buffer size is invalid
     */
    public static void validateBuffer(Buffer buffer, DataType dataType, int expected) {
        boolean isByteBuffer = buffer instanceof ByteBuffer;
        DataType type = DataType.fromBuffer(buffer);
        if (type != dataType && !isByteBuffer) {
            // It's ok if type != datatype and buffer is ByteBuffer,
            // since buffer will be copied into ByteBuffer
            throw new IllegalArgumentException(
                    "The input data type: "
                            + type
                            + " does not match target array data type: "
                            + dataType);
        }

        int remaining = buffer.remaining();
        int expectedSize = isByteBuffer ? dataType.getNumOfBytes() * expected : expected;
        if (remaining < expectedSize) {
            throw new IllegalArgumentException(
                    "The NDArray size is: " + expected + ", but buffer size is: " + remaining);
        }
        if (remaining > expectedSize) {
            logger.warn(
                    "Input buffer size is greater than the NDArray size, please set limit"
                            + " explicitly.");
            buffer.limit(expectedSize);
        }
    }

    // -------------------------- DEFAULT METHODS ---------------------------

    /**
     * Copies data from the source {@code Buffer} to the target {@code ByteBuffer}.
     *
     * @param src    the source {@code Buffer}
     * @param target the target {@code ByteBuffer}
     */
    public static void copyBuffer(Buffer src, ByteBuffer target) {
        target.rewind();
        DataType inputType = DataType.fromBuffer(src);
        switch (inputType) {
            case FLOAT16:
                target.asShortBuffer().put((ShortBuffer) src);
                break;
            case FLOAT32:
                target.asFloatBuffer().put((FloatBuffer) src);
                break;
            case FLOAT64:
                target.asDoubleBuffer().put((DoubleBuffer) src);
                break;
            case UINT8:
            case INT8:
            case BOOLEAN:
                target.put((ByteBuffer) src);
                break;
            case INT32:
                target.asIntBuffer().put((IntBuffer) src);
                break;
            case INT64:
                target.asLongBuffer().put((LongBuffer) src);
                break;
            default:
                throw new AssertionError("Unsupported datatype: " + inputType);
        }
        target.rewind();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        if (delayed == 0) ticker.increment();
        else scopedCount++;
        attached.put(resource, NDHelper.VOID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void detachInternal(String resourceId, AutoCloseable resource) {
        attached.invalidate(resource); // !This might cause eviction is the time is late
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        delayed++;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        if (--delayed == 0) {
            ticker.increment(scopedCount);
            scopedCount = 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Device defaultDevice() {
        return getEngine().defaultDevice();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray create(String[] data, Charset charset, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray create(Shape shape, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray createCSR(Buffer data, long[] indptr, long[] indices, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray createRowSparse(Buffer data, Shape dataShape, long[] indices, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray createCoo(Buffer data, long[][] indices, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDList load(Path path) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name == null ? uid : this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray full(Shape shape, float value, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray arange(float start, float stop, float step, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray eye(int rows, int cols, int k, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray linspace(float start, float stop, int num, boolean endpoint) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomInteger(long low, long high, Shape shape, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomPermutation(long n) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomUniform(float low, float high, Shape shape, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomNormal(float loc, float scale, Shape shape, DataType dataType) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray truncatedNormal(float loc, float scale, Shape shape, DataType dataType) {
        int sampleSize = (int) shape.size();
        double[] dist = new double[sampleSize];

        for (int i = 0; i < sampleSize; i++) {
            double sample = RandomUtils.nextGaussian();
            while (sample < -2 || sample > 2) {
                sample = RandomUtils.nextGaussian();
            }

            dist[i] = sample;
        }

        return create(dist).muli(scale).addi(loc).reshape(shape).toType(dataType, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomMultinomial(int n, NDArray pValues) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray randomMultinomial(int n, NDArray pValues, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sampleNormal(NDArray mu, NDArray sigma) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sampleNormal(NDArray mu, NDArray sigma, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray samplePoisson(NDArray lam) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray samplePoisson(NDArray lam, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sampleGamma(NDArray alpha, NDArray beta) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sampleGamma(NDArray alpha, NDArray beta, Shape shape) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cap() {
        // pass, no cap by default
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDManager getParentManager() {
        return parent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDManager newSubManager() {
        return newSubManager(device);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Device getDevice() {
        return device;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NDArray> getManagedArrays() {
        return attached.asMap().keySet().stream()
                .flatMap(
                        r -> {
                            if (r instanceof NDResource) {
                                return ((NDResource) r)
                                        .getResourceNDArrays().stream();
                            } else if (r instanceof NDManager) {
                                return ((NDManager) r).getManagedArrays().stream();
                            } else {
                                return Stream.empty();
                            }
                        })
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        String parentName = parent == null ? "No Parent" : parent.getName();
        return "Name: "
                + getName()
                + " Parent Name: "
                + parentName
                + " isOpen: "
                + isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invoke(
            String operation, NDArray[] src, NDArray[] dest, PairList<String, ?> params) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDList invoke(String operation, NDList src, PairList<String, ?> params) {
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // pass closing on de-reference
    }

    /**
     * Prints information about this {@link NDManager} and all sub-managers to the console.
     *
     * @param level the level of this {@link NDManager} in the hierarchy
     */
    public void debugDump(int level) {
        StringBuilder sb = new StringBuilder(100);
        for (int i = 0; i < level; ++i) {
            sb.append("    ");
        }
        sb.append("\\--- NDManager(")
                .append(uid.substring(24))
                .append(") resource count: ")
                .append(attached.estimatedSize());
    }

    NDManager getAlternativeManager() {
        return this;
    }

    protected static final class TempResource {

        private final NDResource resource;
        private final NDManager manager;
        private final boolean detached;

        public TempResource(NDResource resource, NDManager manager) {
            this.resource = resource;
            this.manager = manager;
            this.detached = false;
        }

        public void returnResource() {
            try {
                if (!detached) {
                    if (manager.isOpen()) {
                        resource.returnResource(manager);
                    } else {
                        resource.close();
                    }
                }
            } catch (Exception e) {
                logger.error("Temporary resource return failed.", e);
            }
        }
    }

    /**
     * Logical Clock for releasing the Tensors
     */
    static class ManualTicker implements Ticker {
        private long value = 0;

        public void increment() {
            value++;
        }

        public void increment(long tmp) {
            value += tmp;
        }

        @Override
        public long read() {
            return value;
        }
    }

    /**
     * Cleanup remaining tensors after the Thread is dead
     */
    static class NDManagerFinalizeTask implements Runnable {

        private final Cache<AutoCloseable, Object> attached;

        public NDManagerFinalizeTask(BaseNDManager manager) {
            attached = manager.attached;
        }

        @Override
        public void run() {
            attached.asMap().forEach((key, value) -> {
                ((LifeCycleControl) key).destroy();
            });
        }
    }
}
