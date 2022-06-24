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
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.util.Float16Utils;
import ai.djl.util.PairList;
import ai.djl.util.PtNDArrayFinalizeTask;
import ai.djl.util.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.nio.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code BaseNDManager} is the default implementation of {@link NDManager}.
 * It is not managing any closing logic, instead registes all NDArrays with a Cleaner
 */
public abstract class BaseNDManager implements NDManager {
    protected final transient static Cleaner cleaner = Cleaner.create();
    private static final Logger logger = LoggerFactory.getLogger(BaseNDManager.class);
    protected NDManager parent;
    protected NDManager alternativeManager;
    protected String uid;
    protected String name;
    protected Device device;
    protected AtomicBoolean closed = new AtomicBoolean(false);

    protected BaseNDManager(NDManager parent, Device device) {
        this.parent = parent;
        this.device = device == null ? defaultDevice() : device;
        uid = UUID.randomUUID().toString();
        Engine engine = getEngine().getAlternativeEngine();
        if (engine != null) {
            alternativeManager = engine.newBaseManager(Device.cpu());
        }
    }

    /**
     * Checks if the input buffer size is match expected data type.
     *
     * @param buffer   the input buffer
     * @param dataType the desired {@code DataType}
     * @param expected the expected size
     * @throws IllegalArgumentException if buffer size is invalid
     */
    public static void validateBufferSize(Buffer buffer, DataType dataType, int expected) {
        int remaining = buffer.remaining();
        int expectedSize =
                buffer instanceof ByteBuffer ? dataType.getNumOfBytes() * expected : expected;
        if (remaining < expectedSize) {
            throw new IllegalArgumentException(
                    "The NDArray size is: " + expected + ", but buffer size is: " + remaining);
        }
        if (remaining > expectedSize) {
            logger.warn(
                    "Input buffer size is greater than the NDArray size, please set limit explicitly.");
            buffer.limit(expectedSize);
        }
    }

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
    public NDArray zeros(Shape shape, DataType dataType) {
        int size = (int) shape.size();
        ByteBuffer bb = allocateDirect(size * dataType.getNumOfBytes());
        return create(bb, shape, dataType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray ones(Shape shape, DataType dataType) {
        int size = (int) shape.size();
        ByteBuffer bb = allocateDirect(size * dataType.getNumOfBytes());
        for (int i = 0; i < size; ++i) {
            switch (dataType) {
                case FLOAT16:
                    bb.putShort(Float16Utils.ONE);
                    break;
                case FLOAT32:
                    bb.putFloat(1f);
                    break;
                case FLOAT64:
                    bb.putDouble(1d);
                    break;
                case INT32:
                    bb.putInt(1);
                    break;
                case INT64:
                    bb.putLong(1);
                    break;
                case UINT8:
                case INT8:
                    bb.put((byte) 1);
                    break;
                case UNKNOWN:
                default:
                    break;
            }
        }
        bb.rewind();
        return create(bb, shape, dataType);
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
    public boolean isOpen() {
        return !closed.get();
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
     * Main NDArray Logic Implemented here
     */

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
    public synchronized void attachInternal(String resourceId, AutoCloseable resource) {
        if (resource instanceof PtNDArray) {
            cleaner.register(resource, new PtNDArrayFinalizeTask((PtNDArray) resource));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void tempAttachInternal(
            NDManager originalManager, String resourceId, NDResource resource) {
        // Since it is just transfering from one NDArray to next one, use this instead
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void detachInternal(String resourceId) {
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
        this.closed.set(true);
    }

    /**
     * Prints information about this {@link NDManager} and all sub-managers to the console.
     *
     * @param level the level of this {@link NDManager} in the hierarchy
     */
    public void debugDump(int level) {

    }

    NDManager getAlternativeManager() {
        return alternativeManager;
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
                        resource.attach(manager);
                    } else {
                        resource.close();
                    }
                }
            } catch (Exception e) {
                logger.error("Temporary resource return failed.", e);
            }
        }
    }
}
