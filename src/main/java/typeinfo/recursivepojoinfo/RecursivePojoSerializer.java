package typeinfo.recursivepojoinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import typeinfo.DummySerializer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

/**
 * PojoSerializer when there is a recursive List field
 *
 * @param <T>
 */
public class RecursivePojoSerializer<T> extends TypeSerializer<T> {

    private static final Field _clazzField;

    private static final Field _fieldSerializersField;

    private static final Field _fieldsField;

    private static final Field _executionConfigField;

    private static final Field _listElementSerializer;

    static {
        try {
            _clazzField = PojoSerializer.class.getDeclaredField("clazz");
            _fieldSerializersField = PojoSerializer.class.getDeclaredField("fieldSerializers");
            _fieldsField = PojoSerializer.class.getDeclaredField("fields");
            _executionConfigField = PojoSerializer.class.getDeclaredField("executionConfig");
            _listElementSerializer = ListSerializer.class.getDeclaredField("elementSerializer");
            _listElementSerializer.setAccessible(true);
            _clazzField.setAccessible(true);
            _fieldsField.setAccessible(true);
            _fieldSerializersField.setAccessible(true);
            _executionConfigField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private final PojoSerializer<T> actualSerializer;

    private final Class<T> clazz;

    private final TypeSerializer<?>[] fieldSerializers;

    private final ExecutionConfig executionConfig;

    private final Set<Integer> recursiveListFieldIndices; // Indices of recursive List Fields

    private final boolean deSerializationCallback;

    public RecursivePojoSerializer(PojoSerializer<T> pojoSerializer) {
        actualSerializer = pojoSerializer;
        try {
            clazz = (Class<T>) _clazzField.get(pojoSerializer);
            fieldSerializers = (TypeSerializer<?>[]) _fieldSerializersField.get(pojoSerializer);
            executionConfig = (ExecutionConfig) _executionConfigField.get(pojoSerializer);
            recursiveListFieldIndices = new HashSet<>(3);
            for (int i = 0; i < fieldSerializers.length; i++) {
                if (fieldSerializers[i] instanceof ListSerializer && ((ListSerializer<?>) fieldSerializers[i]).getElementSerializer() instanceof DummySerializer) {
                    // This is recursive List Serializer
                    _listElementSerializer.set(fieldSerializers[i], this);
                    recursiveListFieldIndices.add(i);
                }
            }
            deSerializationCallback = DeSerializationListener.class.isAssignableFrom(clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error occured");
        }
    }


    @Override
    public boolean isImmutableType() {
        return actualSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        TypeSerializer<?>[] duplicatedFieldSerializers = duplicateSerializers();
        if (duplicatedFieldSerializers == fieldSerializers) return this;
        try {
            return new RecursivePojoSerializer<>(new PojoSerializer<>(clazz, duplicatedFieldSerializers, (Field[]) _fieldsField.get(actualSerializer), executionConfig));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private TypeSerializer<?>[] duplicateSerializers() {
        boolean stateful = false;
        TypeSerializer<?>[] duplicateSerializers = new TypeSerializer[fieldSerializers.length];

        for (int i = 0; i < fieldSerializers.length; i++) {
            if (!recursiveListFieldIndices.contains(i)) {
                duplicateSerializers[i] = fieldSerializers[i].duplicate();
                if (duplicateSerializers[i] != fieldSerializers[i]) {
                    // at least one of them is stateful
                    stateful = true;
                }
            }
        }

        if (!stateful) {
            // as a small memory optimization, we can share the same object between instances
            return fieldSerializers;
        }
        for (Integer recursiveListFieldIndex : recursiveListFieldIndices) {
            duplicateSerializers[recursiveListFieldIndex] = new ListSerializer<>(new DummySerializer<>());
        }
        return duplicateSerializers;
    }


    @Override
    public T createInstance() {
        return actualSerializer.createInstance();
    }

    @Override
    public T copy(T from) {
        return actualSerializer.copy(from);
    }

    @Override
    public T copy(T from, T reuse) {
        return actualSerializer.copy(from, reuse);
    }

    @Override
    public int getLength() {
        return actualSerializer.getLength();
    }

    @Override
    public void serialize(T value, DataOutputView target) throws IOException {
        actualSerializer.serialize(value, target);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        T obj = actualSerializer.deserialize(source);
        if (deSerializationCallback) ((DeSerializationListener) obj).deserialized();
        return obj;
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        T obj = actualSerializer.deserialize(reuse, source);
        if (deSerializationCallback) ((DeSerializationListener) obj).deserialized();
        return obj;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        actualSerializer.copy(source, target);
    }

    @Override
    public int hashCode() {
        return actualSerializer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RecursivePojoSerializer) {
            return actualSerializer.equals(((RecursivePojoSerializer<?>) obj).actualSerializer);
        } else {
            return false;
        }
    }

    @Override
    public PojoSerializerSnapshot<T> snapshotConfiguration() {
        return actualSerializer.snapshotConfiguration();
    }
}
