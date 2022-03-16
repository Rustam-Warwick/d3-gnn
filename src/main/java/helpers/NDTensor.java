package helpers;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * This is used to wrap around NDArray Types
 * We cannot resort to Kryo serializer since data lies outside JVM
 * Instead array is transient and is always attached to single loop manager
 */
public class NDTensor {
    public byte[] data;
    public Shape shape;
    public DataType dataType;
    public transient NDArray array = null;
    public NDTensor(NDArray tmp){
        this.data = tmp.toByteArray();
        this.dataType = tmp.getDataType();
        this.shape = tmp.getShape();
        this.array = tmp;
    }
    public NDTensor(byte[] data, Shape shape, DataType dataType){
        this.data = data;
        this.shape = shape;
        this.dataType = dataType;
    }
    public NDTensor(byte[] data, Shape shape, NDArray tmp){
        this.data = data;
        this.shape = shape;
        this.array = tmp;
    }

    public NDTensor(){
        this.data = null;
        this.shape = null;
        this.dataType = null;
    }

    public NDArray get(NDManager manager){
        if(Objects.isNull(this.array) || !this.array.getManager().isOpen()){
            this.array = manager.create(ByteBuffer.wrap(this.data),this.shape, this.dataType);
        }
        return this.array;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NDTensor ndTensor = (NDTensor) o;
        return Arrays.equals(data, ndTensor.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    public NDTensor copy(){
        return new NDTensor(this.data, this.shape, this.array);
    }
    public NDTensor deepCopy(){
        return new NDTensor(this.data.clone(), this.shape, this.dataType);
    }
}
