package ai.djl.typeinfo;

import ai.djl.ndarray.NDArray;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class NDArraySerializer extends TypeSerializer<NDArray> {
    public NDArraySerializer() {

    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<NDArray> duplicate() {
        return null;
    }

    @Override
    public NDArray createInstance() {
        return null;
    }

    @Override
    public NDArray copy(NDArray from) {
        return null;
    }

    @Override
    public NDArray copy(NDArray from, NDArray reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(NDArray record, DataOutputView target) throws IOException {

    }

    @Override
    public NDArray deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public NDArray deserialize(NDArray reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<NDArray> snapshotConfiguration() {
        return null;
    }
}
