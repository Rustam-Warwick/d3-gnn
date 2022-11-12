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
package ai.djl.mxnet.engine;

import ai.djl.Device;
import ai.djl.mxnet.jna.JnaUtils;
import ai.djl.ndarray.*;
import ai.djl.ndarray.internal.NDArrayEx;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.ndarray.types.SparseFormat;
import ai.djl.util.NativeResource;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.apache.commons.lang3.NotImplementedException;

import java.lang.ref.Cleaner;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * {@code MxNDArray} is the MXNet implementation of {@link NDArray}.
 */
public class MxNDArray extends NativeResource<Pointer> implements LazyNDArray {
    protected static final Cleaner cleaner = Cleaner.create();
    private final MxNDArrayEx mxNDArrayEx;
    private Device device;
    private SparseFormat sparseFormat;
    private DataType dataType;
    private Shape shape;
    private byte delayed;
    private transient Cleaner.Cleanable cleanable;
    // use Boolean object to maintain three status: false, true
    // and null which means the flag is not set by the native engine yet
    private Boolean hasGradient;

    /**
     * Constructs an MxNDArray from a native handle and metadata (internal. Use {@link NDManager}
     * instead).
     *
     * @param manager     the manager to attach the new array to
     * @param handle      the pointer to the native MxNDArray memory
     * @param device      the device the new array will be located on
     * @param shape       the shape of the new array
     * @param dataType    the dataType of the new array
     * @param hasGradient the gradient status of the new array
     */
    MxNDArray(
            MxNDManager manager,
            Pointer handle,
            Device device,
            Shape shape,
            DataType dataType,
            boolean hasGradient) {
        this(manager, handle);
        this.device = device;
        // shape check
        if (Arrays.stream(shape.getShape()).anyMatch(s -> s < 0)) {
            throw new IllegalArgumentException("The shape must be >= 0");
        }
        this.shape = shape;
        this.dataType = dataType;
        this.hasGradient = hasGradient;
    }

    /**
     * Constructs an MxNDArray from a native handle (internal. Use {@link NDManager} instead).
     *
     * @param manager the manager to attach the new array to
     * @param handle  the pointer to the native MxNDArray memory
     */
    MxNDArray(MxNDManager manager, Pointer handle) {
        super(handle);
        mxNDArrayEx = new MxNDArrayEx(this);
        getManager().attachInternal(null, this);
    }

    /**
     * Constructs a sparse MxNDArray from a native handle (internal. Use {@link NDManager} instead).
     *
     * @param manager the manager to attach the new array to
     * @param handle  the pointer to the native MxNDArray memory
     * @param fmt     the sparse format
     */
    MxNDArray(MxNDManager manager, Pointer handle, SparseFormat fmt) {
        this(manager, handle);
        this.sparseFormat = fmt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MxNDManager getManager() {
        return MxNDManager.getSystemManager().newSubManager(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setName(String name) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType() {
        if (dataType == null) {
            dataType = JnaUtils.getDataType(getHandle());
        }
        return dataType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Device getDevice() {
        if (device == null) {
            device = JnaUtils.getDevice(getHandle());
        }
        return device;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Shape getShape() {
        if (shape == null) {
            shape = JnaUtils.getShape(getHandle());
        }
        return shape;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparseFormat getSparseFormat() {
        if (sparseFormat == null) {
            sparseFormat = JnaUtils.getStorageType(getHandle());
        }
        return sparseFormat;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void attach(NDManager manager) {
        throw new NotImplementedException("Not implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnResource(NDManager manager) {
        throw new NotImplementedException("Not implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void tempAttach(NDManager manager) {
        throw new NotImplementedException("Not Implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void detach() {
        getManager().detachInternal(null, this);
        if (cleanable == null)
            cleanable = cleaner.register(this, new MxNDArray.MxNDArrayFinalizerTask(this)); // Attach to cleaner instead otherwise handled by the LifeCycleManager
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        if (cleanable != null) return;
        if (++delayed == 1) {
            getManager().detachInternal(null, this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        if (cleanable != null || delayed == 0) return;
        if (--delayed == 0) {
            getManager().attachInternal(null, this);
        }
    }

    private NDArray duplicate(
            NDManager manager, Shape shape, DataType dataType, Device device, String name) {
        // TODO get copy parameter
        NDArray array = getManager().create(shape, dataType, device);
        array.setName(name);
        copyTo(array);
        return array;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toDevice(Device device, boolean copy) {
        if (device.equals(getDevice()) && !copy) {
            return this;
        }
        return duplicate(getManager(), getShape(), getDataType(), device, getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toType(DataType dataType, boolean copy) {
        if (dataType.equals(getDataType()) && !copy) {
            return this;
        }
        return duplicate(getManager(), getShape(), dataType, getDevice(), getName());
    }

    /**
     * Computes the gradients of the NDArray w.r.t variables.
     *
     * @param retainGraph whether to retain the computation graph for another backward pass on the
     *                    same graph. By default, the computation history is cleared.
     */
    public void backward(boolean retainGraph) {
        JnaUtils.autogradBackward(new NDList(this), retainGraph ? 1 : 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRequiresGradient(boolean requiresGrad) {
        if ((requiresGrad && hasGradient()) || (!requiresGrad && !hasGradient())) {
            return;
        }
        MxNDArray grad =
                hasGradient() ? (MxNDArray) getGradient() : createGradient(getSparseFormat());
        // DJL go with write as only MXNet support GradReq
        int gradReqValue = requiresGrad ? GradReq.WRITE.getValue() : GradReq.NULL.getValue();
        IntBuffer gradReqBuffer = IntBuffer.allocate(1);
        gradReqBuffer.put(0, gradReqValue);
        JnaUtils.autogradMarkVariables(1, getHandle(), gradReqBuffer, grad.getHandle());
        hasGradient = requiresGrad;
        grad.close();
    }

    private MxNDArray createGradient(SparseFormat format) {
        try (MxNDArray zeros = (MxNDArray) getManager().zeros(getShape(), getDataType(), getDevice())) {
            return (MxNDArray) zeros.toSparse(format);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray getGradient() {
        if (!hasGradient()) {
            throw new IllegalStateException(
                    "No gradient attached to this NDArray, please call array.setRequiresGradient()"
                            + " on your NDArray or block.setInitializer() on your Block");
        }
        Pointer pointer = JnaUtils.getGradient(getHandle());
        return getManager().create(pointer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasGradient() {
        if (hasGradient == null) {
            Pointer pointer = JnaUtils.getGradient(getHandle());
            hasGradient = pointer != null;
        }
        return hasGradient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray stopGradient() {
        Pointer pointer = JnaUtils.detachGradient(getHandle());
        return getManager().create(pointer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] toStringArray(Charset charset) {
        throw new UnsupportedOperationException("String NDArray is not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer toByteBuffer() {
        if (getSparseFormat() != SparseFormat.DENSE) {
            throw new IllegalStateException("Require Dense NDArray, actual " + getSparseFormat());
        }
        Shape sh = getShape();
        DataType dType = getDataType();
        long product = sh.size();
        long len = dType.getNumOfBytes() * product;
        ByteBuffer bb = getManager().allocateDirect(Math.toIntExact(len));
        Pointer pointer = Native.getDirectBufferPointer(bb);
        JnaUtils.syncCopyToCPU(getHandle(), pointer, Math.toIntExact(product));
        return bb;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(Buffer buffer) {
        int size = Math.toIntExact(size());
        DataType type = getDataType();
        BaseNDManager.validateBuffer(buffer, type, size);
        if (buffer.isDirect()) {
            JnaUtils.syncCopyFromCPU(getHandle(), buffer, size);
            return;
        }

        ByteBuffer bb = getManager().allocateDirect(size * type.getNumOfBytes());
        BaseNDManager.copyBuffer(buffer, bb);
        JnaUtils.syncCopyFromCPU(getHandle(), bb, size);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gather(NDArray index, int axis) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray take(NDManager manager, NDArray index) {
        MxOpParams params = new MxOpParams();
        params.add("mode", "wrap");
        return getManager().invoke("take", new NDList(this.flatten(), index), params).singletonOrThrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray put(NDArray index, NDArray data) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(NDArray ndArray) {
        if (!(ndArray instanceof MxNDArray)) {
            throw new IllegalArgumentException("Only MxNDArray is supported.");
        }
        Shape inShape = getShape();
        Shape destShape = ndArray.getShape();
        if (!Arrays.equals(inShape.getShape(), destShape.getShape())) {
            throw new IllegalArgumentException(
                    "shape are diff. Required: " + destShape + ", Actual " + inShape);
        }
        getManager().invoke("_npi_copyto", new NDArray[]{this}, new NDArray[]{ndArray}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray booleanMask(NDArray index, int axis) {
        if (isScalar() || index.isScalar()) {
            throw new IllegalArgumentException("booleanMask didn't support scalar!");
        }
        // TODO remove reshape when MXNet numpy support multi-dim index
        // and boolean NDArray reshape
        Shape remainingDims = getShape().slice(index.getShape().dimension());
        // create a reshape array {-1, remainingDims}
        long[] reshape = new long[remainingDims.dimension() + 1];
        reshape[0] = -1;
        System.arraycopy(remainingDims.getShape(), 0, reshape, 1, remainingDims.dimension());
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        try (NDArray reshaped = this.reshape(reshape);
             NDArray reshapedIndex = index.toType(DataType.INT32, false).reshape(-1);
             NDArray result =
                     getManager().invoke(
                             "_npi_boolean_mask",
                             new NDArray[]{reshaped, reshapedIndex},
                             params)) {
            return result.reshape(reshape);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sequenceMask(NDArray sequenceLength, float value) {
        if (getShape().dimension() < 2 || getShape().isScalar() || getShape().hasZeroDimension()) {
            throw new IllegalArgumentException(
                    "sequenceMask is not supported for NDArray with less than 2 dimensions");
        }
        Shape expectedSequenceLengthShape = new Shape(getShape().get(0));
        if (!sequenceLength.getShape().equals(expectedSequenceLengthShape)) {
            throw new IllegalArgumentException("SequenceLength must be of shape [batchSize]");
        }
        MxOpParams params = new MxOpParams();
        params.add("value", value);
        params.add("use_sequence_length", true);
        params.add("axis", 1);
        return getManager().invoke("_npx_sequence_mask", new NDList(this, sequenceLength), params)
                .head();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sequenceMask(NDArray sequenceLength) {
        return sequenceMask(sequenceLength, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contentEquals(Number number) {
        if (number == null) {
            return false;
        }
        try (NDArray result = eq(number)) {
            return result.all().getBoolean();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contentEquals(NDArray other) {
        if (other == null || (!shapeEquals(other))) {
            return false;
        }
        if (getDataType() != other.getDataType()) {
            return false;
        }
        try (NDArray result = eq(other).toType(DataType.INT32, false)) {
            return result.all().getBoolean();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray eq(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_equal_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray eq(NDArray other) {
        return getManager().invoke("_npi_equal", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray neq(Number other) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", other.toString());
        return getManager().invoke("_npi_not_equal_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray neq(NDArray other) {
        return getManager().invoke("_npi_not_equal", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gt(Number other) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", other.toString());
        return getManager().invoke("_npi_greater_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gt(NDArray other) {
        return getManager().invoke("_npi_greater", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gte(Number other) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", other.toString());
        return getManager().invoke("_npi_greater_equal_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gte(NDArray other) {
        return getManager().invoke("_npi_greater_equal", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray lt(Number other) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", other.toString());
        return getManager().invoke("_npi_less_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray lt(NDArray other) {
        return getManager().invoke("_npi_less", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray lte(Number other) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", other.toString());
        return getManager().invoke("_npi_less_equal_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray lte(NDArray other) {
        return getManager().invoke("_npi_less_equal", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray add(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_add_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray add(NDArray other) {
        return getManager().invoke("_npi_add", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sub(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_subtract_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sub(NDArray other) {
        return getManager().invoke("_npi_subtract", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mul(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_multiply_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mul(NDArray other) {
        return getManager().invoke("_npi_multiply", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toSparse(SparseFormat fmt) {
        if (fmt != SparseFormat.DENSE
                && fmt != SparseFormat.CSR
                && fmt != SparseFormat.ROW_SPARSE) {
            throw new UnsupportedOperationException(fmt + " is not supported");
        }
        if (fmt == getSparseFormat()) {
            return duplicate();
        }
        return castStorage(fmt);
    }

    private NDArray castStorage(SparseFormat fmt) {
        MxOpParams params = new MxOpParams();
        params.setParam("stype", fmt.getType());
        return getManager().invoke("cast_storage", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray div(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_true_divide_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray div(NDArray other) {
        return getManager().invoke("_npi_true_divide", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mod(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_mod_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mod(NDArray other) {
        return getManager().invoke("_npi_mod", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray pow(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_power_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray pow(NDArray other) {
        return getManager().invoke("_npi_power", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray addi(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke("_npi_add_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray addi(NDArray other) {
        getManager().invoke("_npi_add", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray subi(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke("_npi_subtract_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray subi(NDArray other) {
        getManager().invoke("_npi_subtract", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray muli(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke("_npi_multiply_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray muli(NDArray other) {
        getManager().invoke("_npi_multiply", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray divi(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke(
                "_npi_true_divide_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray divi(NDArray other) {
        getManager().invoke("_npi_true_divide", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray modi(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke("_npi_mod_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray modi(NDArray other) {
        getManager().invoke("_npi_mod", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray powi(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        getManager().invoke("_npi_power_scalar", new NDArray[]{this}, new NDArray[]{this}, params);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray powi(NDArray other) {
        getManager().invoke("_npi_power", new NDArray[]{this, other}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray neg() {
        return getManager().invoke("_npi_negative", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray negi() {
        getManager().invoke("_npi_negative", new NDArray[]{this}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sign() {
        return getManager().invoke("_npi_sign", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray signi() {
        getManager().invoke("_npi_sign", new NDArray[]{this}, new NDArray[]{this}, null);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray abs() {
        return getManager().invoke("_npi_absolute", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray square() {
        return getManager().invoke("_npi_square", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sqrt() {
        return getManager().invoke("_npi_sqrt", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray cbrt() {
        return getManager().invoke("_npi_cbrt", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray floor() {
        return getManager().invoke("_npi_floor", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray ceil() {
        return getManager().invoke("_npi_ceil", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray round() {
        return getManager().invoke("round", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray trunc() {
        return getManager().invoke("_npi_trunc", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray exp() {
        return getManager().invoke("_npi_exp", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray gammaln() {
        return getManager().invoke("gammaln", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray log() {
        return getManager().invoke("_npi_log", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray log10() {
        return getManager().invoke("_npi_log10", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray log2() {
        return getManager().invoke("_npi_log2", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sin() {
        return getManager().invoke("_npi_sin", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray cos() {
        return getManager().invoke("_npi_cos", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tan() {
        return getManager().invoke("_npi_tan", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray asin() {
        return getManager().invoke("_npi_arcsin", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray acos() {
        return getManager().invoke("_npi_arccos", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray atan() {
        return getManager().invoke("_npi_arctan", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sinh() {
        return getManager().invoke("_npi_sinh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray cosh() {
        return getManager().invoke("_npi_cosh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tanh() {
        return getManager().invoke("_npi_tanh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray asinh() {
        return getManager().invoke("_npi_arcsinh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray acosh() {
        return getManager().invoke("_npi_arccosh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray atanh() {
        return getManager().invoke("_npi_arctanh", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toDegrees() {
        return getManager().invoke("_npi_degrees", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toRadians() {
        return getManager().invoke("_npi_radians", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray maximum(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_maximum_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray maximum(NDArray other) {
        return getManager().invoke("_npi_maximum", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray minimum(Number n) {
        MxOpParams params = new MxOpParams();
        params.add("scalar", n.toString());
        return getManager().invoke("_npi_minimum_scalar", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray minimum(NDArray other) {
        return getManager().invoke("_npi_minimum", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray max() {
        return getManager().invoke("_np_max", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray max(int[] axes) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        return getManager().invoke("_np_max", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray max(int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_np_max", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray min() {
        return getManager().invoke("_np_min", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray min(int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_np_min", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sum() {
        // TODO current windows doesn't support boolean NDArray
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            DataType target = getDataType();
            if (!target.isFloating()) {
                try (NDArray thisArr = toType(DataType.FLOAT32, false)) {
                    if (target == DataType.BOOLEAN) {
                        target = DataType.INT64;
                    }
                    try (NDArray array = getManager().invoke("_np_sum", thisArr, null)) {
                        return array.toType(target, false);
                    }
                }
            }
        }
        return getManager().invoke("_np_sum", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sum(int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_np_sum", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray prod() {
        return getManager().invoke("_np_prod", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray prod(int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_np_prod", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mean() {
        return getManager().invoke("_npi_mean", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray mean(int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_npi_mean", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray normalize(double p, long dim, double eps) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray rotate90(int times, int[] axes) {
        if (axes.length != 2) {
            throw new IllegalArgumentException("Axes must be 2");
        }
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axes", axes);
        params.addParam("k", times);
        return getManager().invoke("_npi_rot90", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray trace(int offset, int axis1, int axis2) {
        MxOpParams params = new MxOpParams();
        params.addParam("offset", offset);
        params.addParam("axis1", axis1);
        params.addParam("axis2", axis2);
        return getManager().invoke("_np_trace", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDList split(long[] indices, int axis) {
        if (indices.length == 0) {
            return new NDList(this);
        }
        MxOpParams params = new MxOpParams();
        // follow the numpy behavior
        if (indices[0] != 0) {
            long[] tempIndices = new long[indices.length + 1];
            tempIndices[0] = 0;
            System.arraycopy(indices, 0, tempIndices, 1, indices.length);
            indices = tempIndices;
        }
        params.addTupleParam("indices", indices);
        params.addParam("axis", axis);
        params.addParam("squeeze_axis", false);
        return getManager().invoke("_npi_split", new NDList(this), params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray flatten() {
        return reshape(new Shape(Math.toIntExact(size())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray reshape(Shape shape) {
        MxOpParams params = new MxOpParams();
        params.addParam("newshape", shape);
        return getManager().invoke("_np_reshape", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray expandDims(int axis) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npi_expand_dims", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray squeeze() {
        return getManager().invoke("_np_squeeze", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray squeeze(int[] axes) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        return getManager().invoke("_np_squeeze", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray logicalAnd(NDArray other) {
        // TODO switch to numpy op, although current op support zero-dim, scalar
        NDArray thisArr =
                (getDataType() == DataType.BOOLEAN) ? toType(DataType.INT32, false) : this;
        other =
                (other.getDataType() == DataType.BOOLEAN)
                        ? other.toType(DataType.INT32, false)
                        : other;
        return getManager().invoke("broadcast_logical_and", new NDArray[]{thisArr, other}, null)
                .toType(DataType.BOOLEAN, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray logicalOr(NDArray other) {
        // TODO switch to numpy op, although current op support zero-dim, scalar
        NDArray thisArr =
                (getDataType() == DataType.BOOLEAN) ? toType(DataType.INT32, false) : this;
        other =
                (other.getDataType() == DataType.BOOLEAN)
                        ? other.toType(DataType.INT32, false)
                        : other;
        return getManager().invoke("broadcast_logical_or", new NDArray[]{thisArr, other}, null)
                .toType(DataType.BOOLEAN, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray logicalXor(NDArray other) {
        // TODO switch to numpy op, although current op support zero-dim, scalar
        NDArray thisArr =
                (getDataType() == DataType.BOOLEAN) ? toType(DataType.INT32, false) : this;
        other =
                (other.getDataType() == DataType.BOOLEAN)
                        ? other.toType(DataType.INT32, false)
                        : other;
        return getManager().invoke("broadcast_logical_xor", new NDArray[]{thisArr, other}, null)
                .toType(DataType.BOOLEAN, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray logicalNot() {
        return getManager().invoke("_npi_logical_not", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray argSort(int axis, boolean ascending) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        // be careful that MXNet numpy argsort op didn't officially support this param
        params.addParam("is_ascend", ascending);
        params.setDataType(DataType.INT64);
        return getManager().invoke("_npi_argsort", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sort(int axis) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npi_sort", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray sort() {
        return getManager().invoke("_npi_sort", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray softmax(int axis) {
        // MXNet softmax op bug on GPU
        if (isEmpty()) {
            return getManager().create(getShape());
        }
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npx_softmax", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray logSoftmax(int axis) {
        // MXNet logsoftmax op bug on GPU
        if (isEmpty()) {
            return getManager().create(getShape());
        }
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npx_log_softmax", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray cumSum() {
        return getManager().invoke("_np_cumsum", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray cumSum(int axis) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_np_cumsum", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void intern(NDArray replaced) {
        MxNDArray arr = (MxNDArray) replaced;
        Pointer oldHandle = handle.getAndSet(arr.handle.getAndSet(null));
        JnaUtils.waitToRead(oldHandle);
        JnaUtils.freeNdArray(oldHandle);
        // dereference old ndarray
        arr.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray isInfinite() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray inverse() {
        return getManager().invoke("inverse", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray isNaN() {
        return getManager().invoke("_npi_isnan", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray toDense() {
        if (!isSparse()) {
            return duplicate();
        }
        return castStorage(SparseFormat.DENSE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tile(long repeats) {
        // zero-dim
        if (isEmpty()) {
            return duplicate();
        }
        // scalar
        int dim = (isScalar()) ? 1 : getShape().dimension();
        long[] repeatsArray = new long[dim];
        Arrays.fill(repeatsArray, repeats);
        return tile(repeatsArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tile(int axis, long repeats) {
        // scalar
        if (isScalar()) {
            throw new IllegalArgumentException("scalar didn't support specifying axis");
        }
        long[] repeatsArray = new long[getShape().dimension()];
        Arrays.fill(repeatsArray, 1);
        repeatsArray[withAxis(axis)] = repeats;
        return tile(repeatsArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tile(long[] repeats) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("reps", repeats);
        return getManager().invoke("_npi_tile", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray tile(Shape desiredShape) {
        return tile(repeatsToMatchShape(desiredShape));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray repeat(long repeats) {
        // zero-dim
        if (isEmpty()) {
            return duplicate();
        }
        // scalar
        int dim = (isScalar()) ? 1 : getShape().dimension();
        long[] repeatsArray = new long[dim];
        Arrays.fill(repeatsArray, repeats);
        return repeat(repeatsArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray repeat(int axis, long repeats) {
        long[] repeatsArray = new long[getShape().dimension()];
        Arrays.fill(repeatsArray, 1);
        repeatsArray[withAxis(axis)] = repeats;
        return repeat(repeatsArray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray repeat(long[] repeats) {
        // TODO get rid of for loop once bug in MXNet np.repeat is fixed
        NDArray array = this;
        int baseAxis = getShape().dimension() - repeats.length;
        for (int i = 0; i < repeats.length; i++) {
            if (repeats[i] > 1) {
                NDArray previousArray = array;
                MxOpParams params = new MxOpParams();
                params.addParam("repeats", repeats[i]);
                params.addParam("axis", baseAxis + i);
                array = getManager().invoke("_np_repeat", array, params);
                if (previousArray != this) {
                    previousArray.close();
                }
            }
        }
        return array;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray repeat(Shape desiredShape) {
        return repeat(repeatsToMatchShape(desiredShape));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray dot(NDArray other) {
        return getManager().invoke("_np_dot", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray matMul(NDArray other) {
        if (isScalar() || other.isScalar()) {
            throw new IllegalArgumentException("scalar is not allowed for matMul()");
        }
        return getManager().invoke("_npi_matmul", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray clip(Number min, Number max) {
        MxOpParams params = new MxOpParams();
        params.addParam("a_min", min);
        params.addParam("a_max", max);
        return getManager().invoke("_npi_clip", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray swapAxes(int axis1, int axis2) {
        MxOpParams params = new MxOpParams();
        params.addParam("dim1", axis1);
        params.addParam("dim2", axis2);
        return getManager().invoke("_npi_swapaxes", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray flip(int... axes) {
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axis", axes);
        return getManager().invoke("_npi_flip", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray transpose() {
        return getManager().invoke("_np_transpose", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray transpose(int... dimensions) {
        if (Arrays.stream(dimensions).anyMatch(d -> d < 0)) {
            throw new UnsupportedOperationException(
                    "Passing -1 for broadcasting the dimension is not currently supported");
        }
        if (!Arrays.equals(
                Arrays.stream(dimensions).sorted().toArray(),
                IntStream.range(0, getShape().dimension()).toArray())) {
            throw new IllegalArgumentException(
                    "You must include each of the dimensions from 0 until "
                            + getShape().dimension());
        }
        MxOpParams params = new MxOpParams();
        params.addTupleParam("axes", dimensions);
        return getManager().invoke("_np_transpose", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray broadcast(Shape shape) {
        MxOpParams params = new MxOpParams();
        params.setShape(shape);
        return getManager().invoke("_npi_broadcast_to", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray argMax() {
        if (isEmpty()) {
            throw new IllegalArgumentException("attempt to get argMax of an empty NDArray");
        }
        return getManager().invoke("_npi_argmax", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray argMax(int axis) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npi_argmax", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray argMin() {
        if (isEmpty()) {
            throw new IllegalArgumentException("attempt to get argMin of an empty NDArray");
        }
        return getManager().invoke("_npi_argmin", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray argMin(int axis) {
        MxOpParams params = new MxOpParams();
        params.addParam("axis", axis);
        return getManager().invoke("_npi_argmin", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray percentile(Number percentile) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray percentile(Number percentile, int[] dimension) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray median() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray median(int[] axes) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray nonzero() {
        NDArray thisArr =
                (getDataType() == DataType.BOOLEAN) ? toType(DataType.INT32, false) : this;
        return getManager().invoke("_npx_nonzero", thisArr, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray erfinv() {
        return getManager().invoke("erfinv", this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray norm(boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.add("flag", -2);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_npi_norm", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray norm(int ord, int[] axes, boolean keepDims) {
        MxOpParams params = new MxOpParams();
        params.addParam("ord", (double) ord);
        params.addTupleParam("axis", axes);
        params.addParam("keepdims", keepDims);
        return getManager().invoke("_npi_norm", this, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray oneHot(int depth, float onValue, float offValue, DataType dataType) {
        MxOpParams params = new MxOpParams();
        params.add("depth", depth);
        params.add("on_value", onValue);
        params.add("off_value", offValue);
        params.add("dtype", dataType);
        return getManager().invoke("_npx_one_hot", this, params).toType(dataType, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArray batchDot(NDArray other) {
        return getManager().invoke("_npx_batch_dot", new NDArray[]{this, other}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NDArrayEx getNDArrayInternal() {
        return mxNDArrayEx;
    }

    private long[] repeatsToMatchShape(Shape desiredShape) {
        Shape curShape = getShape();
        int dimension = curShape.dimension();
        if (desiredShape.dimension() > dimension) {
            throw new IllegalArgumentException("The desired shape has too many dimensions");
        }
        if (desiredShape.dimension() < dimension) {
            int additionalDimensions = dimension - desiredShape.dimension();
            desiredShape = curShape.slice(0, additionalDimensions).addAll(desiredShape);
        }
        long[] repeats = new long[dimension];
        for (int i = 0; i < dimension; i++) {
            if (curShape.get(i) == 0 || desiredShape.get(i) % curShape.get(i) != 0) {
                throw new IllegalArgumentException(
                        "The desired shape is not a multiple of the original shape");
            }
            repeats[i] = Math.round(Math.ceil((double) desiredShape.get(i) / curShape.get(i)));
        }
        return repeats;
    }

    private int withAxis(int axis) {
        return Math.floorMod(axis, getShape().dimension());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitToRead() {
        JnaUtils.waitToRead(getHandle());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitToWrite() {
        JnaUtils.waitToWrite(getHandle());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitAll() {
        JnaUtils.waitToRead(getHandle());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return handle.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (isReleased()) {
            return "This array is already closed";
        }
        return toDebugString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (cleanable != null) return;
        Pointer pointer = handle.getAndSet(null);
        if (pointer != null) {
            JnaUtils.waitToRead(pointer);
            JnaUtils.freeNdArray(pointer);
        }
        getManager().detachInternal(null, this);
    }

    @Override
    public void destroy() {
        if (cleanable != null) return;
        Pointer pointer = handle.getAndSet(null);
        if (pointer != null) {
            JnaUtils.waitToRead(pointer);
            JnaUtils.freeNdArray(pointer);
        }
    }

    public static class MxNDArrayFinalizerTask implements Runnable {
        private final AtomicReference<Pointer> handle;

        public MxNDArrayFinalizerTask(MxNDArray array) {
            handle = array.handle;
        }

        @Override
        public void run() {
            Pointer pointer = handle.getAndSet(null);
            if (pointer != null) {
                JnaUtils.waitToRead(pointer);
                JnaUtils.freeNdArray(pointer);
            }
        }
    }
}
