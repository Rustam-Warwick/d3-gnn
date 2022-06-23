package typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RecursiveListWrapperPojoSerializer<T> extends TypeSerializer<T> {
    public final PojoSerializer<T> actualSerializer;
    public final int recursiveFieldIndex;
    private final Class<T> clazz;
    private final TypeSerializer<Object>[] fieldSerializers;
    private final TypeSerializer<Object>[] nonRecursiveFieldSerializers;
    private final ExecutionConfig executionConfig;


    public RecursiveListWrapperPojoSerializer(
            Class<T> clazz,
            TypeSerializer<?>[] fieldSerializers,
            Field[] fields,
            ExecutionConfig executionConfig,
            int recursiveFieldIndex) {
        fieldSerializers[recursiveFieldIndex] = new ListSerializer<>(this);
        nonRecursiveFieldSerializers = new TypeSerializer[fieldSerializers.length - 1];
        int i = 0;
        for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
            if (fieldSerializer == fieldSerializers[recursiveFieldIndex]) continue;
            nonRecursiveFieldSerializers[i++] = (TypeSerializer<Object>) fieldSerializer;
        }

        actualSerializer = new PojoSerializer<>(clazz, fieldSerializers, fields, executionConfig);
        this.clazz = checkNotNull(clazz);
        this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
        this.executionConfig = checkNotNull(executionConfig);
        this.recursiveFieldIndex = recursiveFieldIndex;
    }


    @Override
    public boolean isImmutableType() {
        return actualSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        try {
            TypeSerializer<Object>[] duplicateFieldSerializers = duplicateSerializers(fieldSerializers);
            Field fieldsField = PojoSerializer.class.getDeclaredField("fields");
            fieldsField.setAccessible(true);
            Field[] fields = (Field[]) fieldsField.get(actualSerializer);
            return new RecursiveListWrapperPojoSerializer<>(clazz, duplicateFieldSerializers, fields, executionConfig, recursiveFieldIndex);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    private TypeSerializer<Object>[] duplicateSerializers(TypeSerializer<?>[] serializers) {
        boolean stateful = false;
        TypeSerializer<?>[] duplicateSerializers = new TypeSerializer[serializers.length];

        for (int i = 0; i < serializers.length; i++) {
            if (i == recursiveFieldIndex) continue;
            duplicateSerializers[i] = serializers[i].duplicate();
            if (duplicateSerializers[i] != serializers[i]) {
                // at least one of them is stateful
                stateful = true;
            }
        }

        if (!stateful) {
            // as a small memory optimization, we can share the same object between instances
            duplicateSerializers = serializers;
        }
        return (TypeSerializer<Object>[]) duplicateSerializers;
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
        return actualSerializer.deserialize(source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return actualSerializer.deserialize(reuse, source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        actualSerializer.copy(source, target);
    }

    @Override
    public int hashCode() {
        return 31
                * (31 * Arrays.hashCode(nonRecursiveFieldSerializers)
                + Objects.hash(clazz));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RecursiveListWrapperPojoSerializer) {
            RecursiveListWrapperPojoSerializer<?> other = (RecursiveListWrapperPojoSerializer<?>) obj;

            return clazz == other.clazz
                    && Arrays.equals(fieldSerializers, other.fieldSerializers)
                    && Arrays.equals(nonRecursiveFieldSerializers, other.nonRecursiveFieldSerializers);
        } else {
            return false;
        }
    }

    @Override
    public PojoSerializerSnapshot<T> snapshotConfiguration() {
        return actualSerializer.snapshotConfiguration();
    }
}
