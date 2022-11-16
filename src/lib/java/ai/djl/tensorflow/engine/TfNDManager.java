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
package ai.djl.tensorflow.engine;

import ai.djl.Device;
import ai.djl.engine.Engine;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.PtNDManager;
import ai.djl.tensorflow.engine.javacpp.JavacppUtils;
import ai.djl.util.Pair;
import org.tensorflow.internal.c_api.TFE_Context;
import org.tensorflow.internal.c_api.TFE_TensorHandle;
import org.tensorflow.internal.c_api.TF_Tensor;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

@SuppressWarnings("PMD.UseTryWithResources")
public class TfNDManager extends BaseNDManager {

    static final TfNDManager SYSTEM_MANAGER = new SystemManager();

    private TfNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    static TfNDManager getSystemManager() {
        return SYSTEM_MANAGER;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer allocateDirect(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override
    public TfNDArray from(NDArray array) {
        if (array == null || array instanceof TfNDArray) {
            return (TfNDArray) array;
        }
        return create(array.toByteBuffer(), array.getShape(), array.getDataType());
    }

    /** {@inheritDoc} */
    @Override
    public NDArray create(Shape shape, DataType dataType) {
        if (dataType == DataType.STRING) {
            throw new IllegalArgumentException(
                    "Use NDManager.create(String[], Charset, Shape) to create String NDArray.");
        }
        if (shape.dimension() == 0) {
            // TensorFlow does not support empty scalar(emtpy NDArray with 0 dimension)
            // initialize with scalar 0
            return create(0f).toType(dataType, false);
        }
        TFE_TensorHandle handle =
                JavacppUtils.createEmptyTFETensor(shape, dataType, getEagerSession(), device);
        return new TfNDArray(this, handle);
    }

    /** {@inheritDoc} */
    @Override
    public TfNDArray create(Buffer data, Shape shape, DataType dataType) {
        if (dataType == DataType.STRING) {
            throw new IllegalArgumentException(
                    "Use NDManager.create(String[], Charset, Shape) to create String NDArray.");
        }
        int size = Math.toIntExact(shape.size());
        BaseNDManager.validateBuffer(data, dataType, size);
        if (data.isDirect() && data instanceof ByteBuffer) {
            TFE_TensorHandle handle =
                    JavacppUtils.createTFETensorFromByteBuffer(
                            (ByteBuffer) data, shape, dataType, getEagerSession(), device);
            return new TfNDArray(this, handle);
        }
        ByteBuffer buf = allocateDirect(size * dataType.getNumOfBytes());
        copyBuffer(data, buf);
        TFE_TensorHandle handle =
                JavacppUtils.createTFETensorFromByteBuffer(
                        buf, shape, dataType, getEagerSession(), device);
        return new TfNDArray(this, handle);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray create(String[] data, Charset charset, Shape shape) {
        ByteBuffer[] buf = new ByteBuffer[data.length];
        for (int i = 0; i < data.length; ++i) {
            buf[i] = ByteBuffer.wrap(data[i].getBytes(charset));
        }
        return createStringTensor(shape, buf);
    }

    /**
     * Creates a String {@link NDArray} based on the provided shape.
     *
     * @param shape the shape of the String NDArray
     * @param data the flattened String array
     * @return a new instance of {@code NDArray}
     */
    public NDArray createStringTensor(Shape shape, ByteBuffer... data) {
        Pair<TF_Tensor, TFE_TensorHandle> pair =
                JavacppUtils.createStringTensor(shape.getShape(), data);
        return new TfNDArray(this, pair.getValue(), pair.getKey());
    }

    /** {@inheritDoc} */
    @Override
    public final Engine getEngine() {
        return Engine.getEngine(TfEngine.ENGINE_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray zeros(Shape shape, DataType dataType) {
        return full(shape, 0, dataType);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray ones(Shape shape, DataType dataType) {
        return full(shape, 1, dataType);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray full(Shape shape, float value, DataType dataType) {
        try (NDArray valueArr = create(value);
                NDArray castedValueArr = valueArr.toType(dataType, false);
                NDArray dimArr = create(shape.getShape())) {
            return opExecutor("Fill")
                    .addInput(dimArr)
                    .addInput(castedValueArr)
                    .buildSingletonOrThrow();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray arange(float start, float stop, float step, DataType dataType) {
        if (stop <= start && step > 0) {
            return create(new Shape(0), dataType);
        }
        try (NDArray startArr = create(start);
                NDArray stopArr = create(stop);
                NDArray stepArr = create(step);
                NDArray castedStartArr = startArr.toType(dataType, false);
                NDArray castedStopArr = stopArr.toType(dataType, false);
                NDArray castedStepArr = stepArr.toType(dataType, false)) {
            return opExecutor("Range")
                    .addInput(castedStartArr)
                    .addInput(castedStopArr)
                    .addInput(castedStepArr)
                    .buildSingletonOrThrow();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray eye(int rows, int cols, int k, DataType dataType) {
        try (NDArray ones = ones(new Shape(Math.min(rows, cols)), dataType);
                NDArray kArr = create(k);
                NDArray rowsArr = create(rows);
                NDArray colsArr = create(cols);
                NDArray zeros = zeros(new Shape(), dataType)) {
            return opExecutor("MatrixDiagV3")
                    .addInput(ones)
                    .addInput(kArr)
                    .addInput(rowsArr)
                    .addInput(colsArr)
                    .addInput(zeros)
                    .buildSingletonOrThrow();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray linspace(float start, float stop, int num, boolean endpoint) {
        if (num < 0) {
            throw new IllegalArgumentException("number of samples must be non-negative.");
        }
        if (num == 0) {
            return create(new Shape(0));
        }
        if (!endpoint && num > 1) {
            stop -= (int) ((stop - start) / num);
        }
        try (NDArray startArr = create(start);
                NDArray stopArr = create(stop);
                NDArray numArr = create(num)) {
            return opExecutor("LinSpace")
                    .addInput(startArr)
                    .addInput(stopArr)
                    .addInput(numArr)
                    .buildSingletonOrThrow();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray randomUniform(float low, float high, Shape shape, DataType dataType) {
        if (DataType.STRING.equals(dataType)) {
            throw new IllegalArgumentException("String data type is not supported!");
        }
        NDArray axes = create(shape.getShape());
        TfOpExecutor opBuilder =
                opExecutor("RandomUniform").addInput(axes).addParam("dtype", dataType);
        Integer seed = getEngine().getSeed();
        if (seed != null) {
            // seed1 is graph-level seed
            // set it to default graph seed used by tensorflow
            // https://github.com/tensorflow/tensorflow/blob/85c8b2a817f95a3e979ecd1ed95bff1dc1335cff/tensorflow/python/framework/random_seed.py#L31
            opBuilder.addParam("seed", 87654321);
            // seed2 is op-level seed
            opBuilder.addParam("seed2", seed);
        }
        try (NDArray array = opBuilder.buildSingletonOrThrow();
                NDArray temp = array.mul(high - low)) {
            return temp.add(low);
        } finally {
            axes.close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray randomNormal(float loc, float scale, Shape shape, DataType dataType) {
        if (DataType.STRING.equals(dataType)) {
            throw new IllegalArgumentException("String data type is not supported!");
        }
        NDArray axes = create(shape.getShape());
        TfOpExecutor opBuilder =
                opExecutor("RandomStandardNormal").addInput(axes).addParam("dtype", dataType);
        Integer seed = getEngine().getSeed();
        if (seed != null) {
            // seed1 is graph-level seed
            // set it to default graph seed used by tensorflow
            // https://github.com/tensorflow/tensorflow/blob/85c8b2a817f95a3e979ecd1ed95bff1dc1335cff/tensorflow/python/framework/random_seed.py#L31
            opBuilder.addParam("seed", 87654321);
            opBuilder.addParam("seed2", seed);
        }
        try (NDArray array = opBuilder.buildSingletonOrThrow();
                NDArray temp = array.mul(scale)) {
            return temp.add(loc);
        } finally {
            axes.close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public NDArray truncatedNormal(float loc, float scale, Shape shape, DataType dataType) {
        if (DataType.STRING.equals(dataType)) {
            throw new IllegalArgumentException("String data type is not supported!");
        }
        NDArray axes = create(shape.getShape());
        TfOpExecutor opBuilder =
                opExecutor("TruncatedNormal").addInput(axes).addParam("dtype", dataType);
        Integer seed = getEngine().getSeed();
        if (seed != null) {
            // seed1 is graph-level seed
            // set it to default graph seed used by tensorflow
            // https://github.com/tensorflow/tensorflow/blob/85c8b2a817f95a3e979ecd1ed95bff1dc1335cff/tensorflow/python/framework/random_seed.py#L31
            opBuilder.addParam("seed", 87654321);
            opBuilder.addParam("seed2", seed);
        }
        try (NDArray array = opBuilder.buildSingletonOrThrow();
                NDArray temp = array.mul(scale)) {
            return temp.add(loc);
        } finally {
            axes.close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public TfNDManager newSubManager(Device device) {
        BaseNDManager manager = ND_MANAGER_THREAD_LOCAL.get();
        if(manager == null){
            manager = new TfNDManager(SYSTEM_MANAGER, SYSTEM_MANAGER.device);
            ND_MANAGER_THREAD_LOCAL.set(manager);
        }
        return (TfNDManager) manager;
    }

    public TFE_Context getEagerSession() {
        return ((TfEngine) getEngine()).getEagerSession();
    }

    public TfOpExecutor opExecutor(String operation) {
        return new TfOpExecutor(this, getEagerSession(), operation);
    }

    private static final class SystemManager extends TfNDManager implements SystemNDManager {
        SystemManager() {
            super(null, null);
        }
    }
}
