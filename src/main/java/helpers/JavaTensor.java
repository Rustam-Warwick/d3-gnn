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
public class JavaTensor implements NDArray {
    public final NDArray array;

    private transient static final Cleaner tensorCleaner = Cleaner.create();
    public JavaTensor(){
        this.array = null;
    }

    public JavaTensor(NDArray arr){
        this.array = arr;
        tensorCleaner.register(this, new TensorCleaner(this.array));
    }

    /**
     * Wraps normal tensor in a JavaTensor, but nesting JavaTensors is not allowed
     * @param array
     * @return
     */
    public static JavaTensor of(NDArray array){
        if(array instanceof JavaTensor) return (JavaTensor) array;
        return new JavaTensor(array);
    }

    private static class TensorCleaner implements Runnable{
        private final NDArray resource;
        public TensorCleaner(NDArray resource){
            this.resource = resource;
        }

        @Override
        public void run() {
            resource.close();
        }
    }


    public static NDArray decode(NDManager manager, byte[] byteArray) {
        return NDArray.decode(manager, byteArray);
    }

    @Override
    public String getName() {
        return array.getName();
    }

    @Override
    public void setName(String name) {
        array.setName(name);
    }

    @Override
    public String getUid() {
        return array.getUid();
    }

    @Override
    public DataType getDataType() {
        return array.getDataType();
    }

    @Override
    public Device getDevice() {
        return array.getDevice();
    }

    @Override
    public Shape getShape() {
        return array.getShape();
    }

    @Override
    public SparseFormat getSparseFormat() {
        return array.getSparseFormat();
    }

    @Override
    public boolean isSparse() {
        return array.isSparse();
    }

    @Override
    public boolean isScalar() {
        return array.isScalar();
    }

    @Override
    public byte[] encode() {
        return array.encode();
    }

    @Override
    public NDArray toDevice(Device device, boolean copy) {
        return array.toDevice(device, copy);
    }

    @Override
    public NDArray toType(DataType dataType, boolean copy) {
        return array.toType(dataType, copy);
    }

    @Override
    public void setRequiresGradient(boolean requiresGrad) {
        array.setRequiresGradient(requiresGrad);
    }

    @Override
    public NDArray getGradient() {
        return array.getGradient();
    }

    @Override
    public boolean hasGradient() {
        return array.hasGradient();
    }

    @Override
    public NDArray stopGradient() {
        return array.stopGradient();
    }

    @Override
    public NDArray scaleGradient(double scale) {
        return array.scaleGradient(scale);
    }

    @Override
    public long size(int axis) {
        return array.size(axis);
    }

    @Override
    public long size() {
        return array.size();
    }

    @Override
    public double[] toDoubleArray() {
        return array.toDoubleArray();
    }

    @Override
    public float[] toFloatArray() {
        return array.toFloatArray();
    }

    @Override
    public int[] toIntArray() {
        return array.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return array.toLongArray();
    }

    @Override
    public byte[] toByteArray() {
        return array.toByteArray();
    }

    @Override
    public int[] toUint8Array() {
        return array.toUint8Array();
    }

    @Override
    public boolean[] toBooleanArray() {
        return array.toBooleanArray();
    }

    @Override
    public String[] toStringArray() {
        return array.toStringArray();
    }

    @Override
    public String[] toStringArray(Charset charset) {
        return array.toStringArray(charset);
    }

    @Override
    public Number[] toArray() {
        return array.toArray();
    }

    @Override
    public void set(Buffer data) {
        array.set(data);
    }

    @Override
    public void set(float[] data) {
        array.set(data);
    }

    @Override
    public void set(int[] data) {
        array.set(data);
    }

    @Override
    public void set(double[] data) {
        array.set(data);
    }

    @Override
    public void set(long[] data) {
        array.set(data);
    }

    @Override
    public void set(byte[] data) {
        array.set(data);
    }

    @Override
    public void set(NDIndex index, NDArray value) {
        array.set(index, value);
    }

    @Override
    public void set(NDIndex index, Number value) {
        array.set(index, value);
    }

    @Override
    public void set(NDIndex index, Function<NDArray, NDArray> function) {
        array.set(index, function);
    }

    @Override
    public void set(NDArray index, Number value) {
        array.set(index, value);
    }

    @Override
    public void setScalar(NDIndex index, Number value) {
        array.setScalar(index, value);
    }

    @Override
    public NDArray get(NDIndex index) {
        return array.get(index);
    }

    @Override
    public NDArray get(String indices, Object... args) {
        return array.get(indices, args);
    }

    @Override
    public NDArray get(long... indices) {
        return array.get(indices);
    }

    @Override
    public NDArray get(NDArray index) {
        return array.get(index);
    }

    @Override
    public NDArray getScalar(long... indices) {
        return array.getScalar(indices);
    }

    @Override
    public long getLong(long... indices) {
        return array.getLong(indices);
    }

    @Override
    public double getDouble(long... indices) {
        return array.getDouble(indices);
    }

    @Override
    public float getFloat(long... indices) {
        return array.getFloat(indices);
    }

    @Override
    public int getInt(long... indices) {
        return array.getInt(indices);
    }

    @Override
    public byte getByte(long... indices) {
        return array.getByte(indices);
    }

    @Override
    public int getUint8(long... indices) {
        return array.getUint8(indices);
    }

    @Override
    public boolean getBoolean(long... indices) {
        return array.getBoolean(indices);
    }

    @Override
    public void copyTo(NDArray array) {
        this.array.copyTo(array);
    }

    @Override
    public NDArray duplicate() {
        return JavaTensor.of(array.duplicate());
    }

    @Override
    public NDArray booleanMask(NDArray index) {
        return array.booleanMask(index);
    }

    @Override
    public NDArray booleanMask(NDArray index, int axis) {
        return array.booleanMask(index, axis);
    }

    @Override
    public NDArray sequenceMask(NDArray sequenceLength, float value) {
        return array.sequenceMask(sequenceLength, value);
    }

    @Override
    public NDArray sequenceMask(NDArray sequenceLength) {
        return array.sequenceMask(sequenceLength);
    }

    @Override
    public NDArray zerosLike() {
        return array.zerosLike();
    }

    @Override
    public NDArray onesLike() {
        return array.onesLike();
    }

    @Override
    public NDArray like() {
        return array.like();
    }

    @Override
    public boolean contentEquals(Number number) {
        return array.contentEquals(number);
    }

    @Override
    public boolean contentEquals(NDArray other) {
        return array.contentEquals(other);
    }

    @Override
    public boolean shapeEquals(NDArray other) {
        return array.shapeEquals(other);
    }

    @Override
    public boolean allClose(NDArray other) {
        return array.allClose(other);
    }

    @Override
    public boolean allClose(NDArray other, double rtol, double atol, boolean equalNan) {
        return array.allClose(other, rtol, atol, equalNan);
    }

    @Override
    public NDArray eq(Number n) {
        return array.eq(n);
    }

    @Override
    public NDArray eq(NDArray other) {
        return array.eq(other);
    }

    @Override
    public NDArray neq(Number n) {
        return array.neq(n);
    }

    @Override
    public NDArray neq(NDArray other) {
        return array.neq(other);
    }

    @Override
    public NDArray gt(Number n) {
        return array.gt(n);
    }

    @Override
    public NDArray gt(NDArray other) {
        return array.gt(other);
    }

    @Override
    public NDArray gte(Number n) {
        return array.gte(n);
    }

    @Override
    public NDArray gte(NDArray other) {
        return array.gte(other);
    }

    @Override
    public NDArray lt(Number n) {
        return array.lt(n);
    }

    @Override
    public NDArray lt(NDArray other) {
        return array.lt(other);
    }

    @Override
    public NDArray lte(Number n) {
        return array.lte(n);
    }

    @Override
    public NDArray lte(NDArray other) {
        return array.lte(other);
    }

    @Override
    public NDArray add(Number n) {
        return array.add(n);
    }

    @Override
    public NDArray add(NDArray other) {
        return array.add(other);
    }

    @Override
    public NDArray sub(Number n) {
        return array.sub(n);
    }

    @Override
    public NDArray sub(NDArray other) {
        return array.sub(other);
    }

    @Override
    public NDArray mul(Number n) {
        return array.mul(n);
    }

    @Override
    public NDArray mul(NDArray other) {
        return array.mul(other);
    }

    @Override
    public NDArray div(Number n) {
        return array.div(n);
    }

    @Override
    public NDArray div(NDArray other) {
        return array.div(other);
    }

    @Override
    public NDArray mod(Number n) {
        return array.mod(n);
    }

    @Override
    public NDArray mod(NDArray other) {
        return array.mod(other);
    }

    @Override
    public NDArray pow(Number n) {
        return array.pow(n);
    }

    @Override
    public NDArray pow(NDArray other) {
        return array.pow(other);
    }

    @Override
    public NDArray addi(Number n) {
        return array.addi(n);
    }

    @Override
    public NDArray addi(NDArray other) {
        return array.addi(other);
    }

    @Override
    public NDArray subi(Number n) {
        return array.subi(n);
    }

    @Override
    public NDArray subi(NDArray other) {
        return array.subi(other);
    }

    @Override
    public NDArray muli(Number n) {
        return array.muli(n);
    }

    @Override
    public NDArray muli(NDArray other) {
        return array.muli(other);
    }

    @Override
    public NDArray divi(Number n) {
        return array.divi(n);
    }

    @Override
    public NDArray divi(NDArray other) {
        return array.divi(other);
    }

    @Override
    public NDArray modi(Number n) {
        return array.modi(n);
    }

    @Override
    public NDArray modi(NDArray other) {
        return array.modi(other);
    }

    @Override
    public NDArray powi(Number n) {
        return array.powi(n);
    }

    @Override
    public NDArray powi(NDArray other) {
        return array.powi(other);
    }

    @Override
    public NDArray sign() {
        return array.sign();
    }

    @Override
    public NDArray signi() {
        return array.signi();
    }

    @Override
    public NDArray maximum(Number n) {
        return array.maximum(n);
    }

    @Override
    public NDArray maximum(NDArray other) {
        return array.maximum(other);
    }

    @Override
    public NDArray minimum(Number n) {
        return array.minimum(n);
    }

    @Override
    public NDArray minimum(NDArray other) {
        return array.minimum(other);
    }

    @Override
    public NDArray neg() {
        return array.neg();
    }

    @Override
    public NDArray negi() {
        return array.negi();
    }

    @Override
    public NDArray abs() {
        return array.abs();
    }

    @Override
    public NDArray square() {
        return array.square();
    }

    @Override
    public NDArray sqrt() {
        return array.sqrt();
    }

    @Override
    public NDArray cbrt() {
        return array.cbrt();
    }

    @Override
    public NDArray floor() {
        return array.floor();
    }

    @Override
    public NDArray ceil() {
        return array.ceil();
    }

    @Override
    public NDArray round() {
        return array.round();
    }

    @Override
    public NDArray trunc() {
        return array.trunc();
    }

    @Override
    public NDArray exp() {
        return array.exp();
    }

    @Override
    public NDArray log() {
        return array.log();
    }

    @Override
    public NDArray log10() {
        return array.log10();
    }

    @Override
    public NDArray log2() {
        return array.log2();
    }

    @Override
    public NDArray sin() {
        return array.sin();
    }

    @Override
    public NDArray cos() {
        return array.cos();
    }

    @Override
    public NDArray tan() {
        return array.tan();
    }

    @Override
    public NDArray asin() {
        return array.asin();
    }

    @Override
    public NDArray acos() {
        return array.acos();
    }

    @Override
    public NDArray atan() {
        return array.atan();
    }

    @Override
    public NDArray sinh() {
        return array.sinh();
    }

    @Override
    public NDArray cosh() {
        return array.cosh();
    }

    @Override
    public NDArray tanh() {
        return array.tanh();
    }

    @Override
    public NDArray asinh() {
        return array.asinh();
    }

    @Override
    public NDArray acosh() {
        return array.acosh();
    }

    @Override
    public NDArray atanh() {
        return array.atanh();
    }

    @Override
    public NDArray toDegrees() {
        return array.toDegrees();
    }

    @Override
    public NDArray toRadians() {
        return array.toRadians();
    }

    @Override
    public NDArray max() {
        return array.max();
    }

    @Override
    public NDArray max(int[] axes) {
        return array.max(axes);
    }

    @Override
    public NDArray max(int[] axes, boolean keepDims) {
        return array.max(axes, keepDims);
    }

    @Override
    public NDArray min() {
        return array.min();
    }

    @Override
    public NDArray min(int[] axes) {
        return array.min(axes);
    }

    @Override
    public NDArray min(int[] axes, boolean keepDims) {
        return array.min(axes, keepDims);
    }

    @Override
    public NDArray sum() {
        return array.sum();
    }

    @Override
    public NDArray sum(int[] axes) {
        return array.sum(axes);
    }

    @Override
    public NDArray sum(int[] axes, boolean keepDims) {
        return array.sum(axes, keepDims);
    }

    @Override
    public NDArray prod() {
        return array.prod();
    }

    @Override
    public NDArray prod(int[] axes) {
        return array.prod(axes);
    }

    @Override
    public NDArray prod(int[] axes, boolean keepDims) {
        return array.prod(axes, keepDims);
    }

    @Override
    public NDArray mean() {
        return array.mean();
    }

    @Override
    public NDArray mean(int[] axes) {
        return array.mean(axes);
    }

    @Override
    public NDArray mean(int[] axes, boolean keepDims) {
        return array.mean(axes, keepDims);
    }

    @Override
    public NDArray rotate90(int times, int[] axes) {
        return array.rotate90(times, axes);
    }

    @Override
    public NDArray trace() {
        return array.trace();
    }

    @Override
    public NDArray trace(int offset) {
        return array.trace(offset);
    }

    @Override
    public NDArray trace(int offset, int axis1, int axis2) {
        return array.trace(offset, axis1, axis2);
    }

    @Override
    public NDList split(long sections) {
        return array.split(sections);
    }

    @Override
    public NDList split(long[] indices) {
        return array.split(indices);
    }

    @Override
    public NDList split(long sections, int axis) {
        return array.split(sections, axis);
    }

    @Override
    public NDList split(long[] indices, int axis) {
        return array.split(indices, axis);
    }

    @Override
    public NDArray flatten() {
        return array.flatten();
    }

    @Override
    public NDArray reshape(long... newShape) {
        return array.reshape(newShape);
    }

    @Override
    public NDArray reshape(Shape shape) {
        return array.reshape(shape);
    }

    @Override
    public NDArray expandDims(int axis) {
        return array.expandDims(axis);
    }

    @Override
    public NDArray squeeze() {
        return array.squeeze();
    }

    @Override
    public NDArray squeeze(int axis) {
        return array.squeeze(axis);
    }

    @Override
    public NDArray squeeze(int[] axes) {
        return array.squeeze(axes);
    }

    @Override
    public NDArray stack(NDArray array) {
        return this.array.stack(array);
    }

    @Override
    public NDArray stack(NDArray array, int axis) {
        return this.array.stack(array, axis);
    }

    @Override
    public NDArray concat(NDArray array) {
        return this.array.concat(array);
    }

    @Override
    public NDArray concat(NDArray array, int axis) {
        return this.array.concat(array, axis);
    }

    @Override
    public NDArray logicalAnd(NDArray other) {
        return array.logicalAnd(other);
    }

    @Override
    public NDArray logicalOr(NDArray other) {
        return array.logicalOr(other);
    }

    @Override
    public NDArray logicalXor(NDArray other) {
        return array.logicalXor(other);
    }

    @Override
    public NDArray logicalNot() {
        return array.logicalNot();
    }

    @Override
    public NDArray argSort() {
        return array.argSort();
    }

    @Override
    public NDArray argSort(int axis) {
        return array.argSort(axis);
    }

    @Override
    public NDArray argSort(int axis, boolean ascending) {
        return array.argSort(axis, ascending);
    }

    @Override
    public NDArray sort() {
        return array.sort();
    }

    @Override
    public NDArray sort(int axis) {
        return array.sort(axis);
    }

    @Override
    public NDArray softmax(int axis) {
        return array.softmax(axis);
    }

    @Override
    public NDArray logSoftmax(int axis) {
        return array.logSoftmax(axis);
    }

    @Override
    public NDArray cumSum() {
        return array.cumSum();
    }

    @Override
    public NDArray cumSum(int axis) {
        return array.cumSum(axis);
    }

    @Override
    public void intern(NDArray replaced) {
        array.intern(replaced);
    }

    @Override
    public NDArray isInfinite() {
        return array.isInfinite();
    }

    @Override
    public NDArray isNaN() {
        return array.isNaN();
    }

    @Override
    public NDArray tile(long repeats) {
        return array.tile(repeats);
    }

    @Override
    public NDArray tile(int axis, long repeats) {
        return array.tile(axis, repeats);
    }

    @Override
    public NDArray tile(long[] repeats) {
        return array.tile(repeats);
    }

    @Override
    public NDArray tile(Shape desiredShape) {
        return array.tile(desiredShape);
    }

    @Override
    public NDArray repeat(long repeats) {
        return array.repeat(repeats);
    }

    @Override
    public NDArray repeat(int axis, long repeats) {
        return array.repeat(axis, repeats);
    }

    @Override
    public NDArray repeat(long[] repeats) {
        return array.repeat(repeats);
    }

    @Override
    public NDArray repeat(Shape desiredShape) {
        return array.repeat(desiredShape);
    }

    @Override
    public NDArray dot(NDArray other) {
        return array.dot(other);
    }

    @Override
    public NDArray matMul(NDArray other) {
        return array.matMul(other);
    }

    @Override
    public NDArray clip(Number min, Number max) {
        return array.clip(min, max);
    }

    @Override
    public NDArray swapAxes(int axis1, int axis2) {
        return array.swapAxes(axis1, axis2);
    }

    @Override
    public NDArray flip(int... axes) {
        return array.flip(axes);
    }

    @Override
    public NDArray transpose() {
        return array.transpose();
    }

    @Override
    public NDArray transpose(int... axes) {
        return array.transpose(axes);
    }

    @Override
    public NDArray broadcast(Shape shape) {
        return array.broadcast(shape);
    }

    @Override
    public NDArray broadcast(long... shape) {
        return array.broadcast(shape);
    }

    @Override
    public NDArray argMax() {
        return array.argMax();
    }

    @Override
    public NDArray argMax(int axis) {
        return array.argMax(axis);
    }

    @Override
    public NDArray argMin() {
        return array.argMin();
    }

    @Override
    public NDArray argMin(int axis) {
        return array.argMin(axis);
    }

    @Override
    public NDArray percentile(Number percentile) {
        return array.percentile(percentile);
    }

    @Override
    public NDArray percentile(Number percentile, int[] axes) {
        return array.percentile(percentile, axes);
    }

    @Override
    public NDArray median() {
        return array.median();
    }

    @Override
    public NDArray median(int[] axes) {
        return array.median(axes);
    }

    @Override
    public NDArray toDense() {
        return array.toDense();
    }

    @Override
    public NDArray toSparse(SparseFormat fmt) {
        return array.toSparse(fmt);
    }

    @Override
    public NDArray nonzero() {
        return array.nonzero();
    }

    @Override
    public boolean isEmpty() {
        return array.isEmpty();
    }

    @Override
    public NDArray all() {
        return array.all();
    }

    @Override
    public NDArray any() {
        return array.any();
    }

    @Override
    public NDArray none() {
        return array.none();
    }

    @Override
    public NDArray countNonzero() {
        return array.countNonzero();
    }

    @Override
    public NDArray countNonzero(int axis) {
        return array.countNonzero(axis);
    }

    @Override
    public NDArray erfinv() {
        return array.erfinv();
    }

    @Override
    public NDArrayEx getNDArrayInternal() {
        return array.getNDArrayInternal();
    }

    @Override
    public String toDebugString() {
        return array.toDebugString();
    }

    @Override
    public String toDebugString(int maxSize, int maxDepth, int maxRows, int maxColumns) {
        return array.toDebugString(maxSize, maxDepth, maxRows, maxColumns);
    }

    @Override
    public void close() {
        array.close();
    }

    @Override
    public NDArray norm() {
        return array.norm();
    }

    @Override
    public NDArray norm(int[] axes) {
        return array.norm(axes);
    }

    @Override
    public NDArray norm(boolean keepDims) {
        return array.norm(keepDims);
    }

    @Override
    public NDArray norm(int[] axes, boolean keepDims) {
        return array.norm(axes, keepDims);
    }

    @Override
    public NDArray norm(int ord, int[] axes, boolean keepDims) {
        return array.norm(ord, axes, keepDims);
    }

    @Override
    public NDArray oneHot(int depth) {
        return array.oneHot(depth);
    }

    @Override
    public NDArray oneHot(int depth, DataType dataType) {
        return array.oneHot(depth, dataType);
    }

    @Override
    public NDArray oneHot(int depth, float onValue, float offValue, DataType dataType) {
        return array.oneHot(depth, onValue, offValue, dataType);
    }

    @Override
    public NDArray batchDot(NDArray other) {
        return array.batchDot(other);
    }

    @Override
    public NDManager getManager() {
        return array.getManager();
    }

    @Override
    public void attach(NDManager manager) {
        array.attach(manager);
    }

    @Override
    public void tempAttach(NDManager manager) {
        array.tempAttach(manager);
    }

    @Override
    public void detach() {
        array.detach();
    }

    @Override
    public byte[] getAsBytes() {
        return array.getAsBytes();
    }

    @Override
    public String getAsString() {
        return array.getAsString();
    }

    @Override
    public Object getAsObject() {
        return array.getAsObject();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return array.toByteBuffer();
    }

    public static BytesSupplier wrap(byte[] buf) {
        return BytesSupplier.wrap(buf);
    }

    public static BytesSupplier wrap(String value) {
        return BytesSupplier.wrap(value);
    }

    public static BytesSupplier wrapAsJson(Object object) {
        return BytesSupplier.wrapAsJson(object);
    }
}
