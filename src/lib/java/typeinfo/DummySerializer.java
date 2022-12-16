package typeinfo;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class DummySerializer<T> extends TypeSerializer<T> {
    @Override
    public boolean isImmutableType() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public TypeSerializer<T> duplicate() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public T createInstance() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public T copy(T from) {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public T copy(T from, T reuse) {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public int getLength() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public boolean equals(Object obj) {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public int hashCode() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");

    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        throw new NotImplementedException("Dummy Serializer is dummyy!");
    }
}
