package typeinfo.setinfo;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SetSerializer<T> extends TypeSerializer<Set<T>> {

    private final TypeSerializer<T> elementSerializer;

    public SetSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer
                ? this
                : new SetSerializer<>(duplicateElement);
    }

    @Override
    public Set<T> createInstance() {
        return new HashSet<>(0);
    }

    @Override
    public Set<T> copy(Set<T> from) {
        return new HashSet<>(from);
    }

    @Override
    public Set<T> copy(Set<T> from, Set<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Set<T> record, DataOutputView target) throws IOException {
        final int size = record.size();
        target.writeInt(size);

        for (T element : record) {
            elementSerializer.serialize(element, target);
        }
    }

    @Override
    public Set<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        // create new list with (size + 1) capacity to prevent expensive growth when a single
        // element is added
        final Set<T> list = new HashSet<>(size + 1);
        for (int i = 0; i < size; i++) {
            list.add(elementSerializer.deserialize(source));
        }
        return list;
    }

    @Override
    public Set<T> deserialize(Set<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int num = source.readInt();
        target.writeInt(num);
        for (int i = 0; i < num; i++) {
            elementSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                && obj.getClass() == getClass()
                && elementSerializer.equals(((SetSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Set<T>> snapshotConfiguration() {
        return new SetSerializerSnapshot<T>(this);
    }
}
