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

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RecursiveListFieldPojoSerializer<T> extends TypeSerializer<T> {
    private final PojoSerializer<T> actualSerializer;
    private final Class<T> clazz;
    private final TypeSerializer<?>[] fieldSerializers;
    private final TypeSerializer<?>[] fieldSerializerswithNullRecursive;
    private final ExecutionConfig executionConfig;


    public RecursiveListFieldPojoSerializer(
            Class<T> clazz,
            TypeSerializer<?>[] fieldSerializers,
            Field[] fields,
            ExecutionConfig executionConfig
    ) {
        fieldSerializerswithNullRecursive = Arrays.copyOf(fieldSerializers, fieldSerializers.length);
        for (int i = 0; i < fieldSerializers.length; i++) {
            if (fieldSerializers[i] == null) {
                // Meant to be recursive
                fieldSerializers[i] = new ListSerializer<>(this);
            }
        }
        actualSerializer = new PojoSerializer<>(clazz, fieldSerializers, fields, executionConfig);
        this.clazz = checkNotNull(clazz);
        this.fieldSerializers = checkNotNull(fieldSerializers);
        this.executionConfig = checkNotNull(executionConfig);
    }


    @Override
    public boolean isImmutableType() {
        return actualSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        try {
            TypeSerializer<?>[] duplicateFieldSerializers = duplicateSerializers(fieldSerializerswithNullRecursive);
            Field fieldsField = PojoSerializer.class.getDeclaredField("fields");
            fieldsField.setAccessible(true);
            Field[] fields = (Field[]) fieldsField.get(actualSerializer);
            return new RecursiveListFieldPojoSerializer<>(clazz, duplicateFieldSerializers, fields, executionConfig);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    private TypeSerializer<?>[] duplicateSerializers(TypeSerializer<?>[] serializers) {
        boolean stateful = false;
        TypeSerializer<?>[] duplicateSerializers = new TypeSerializer[serializers.length];

        for (int i = 0; i < serializers.length; i++) {
            if (serializers[i] == null) continue;
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
        return actualSerializer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RecursiveListFieldPojoSerializer) {
            return actualSerializer.equals(((RecursiveListFieldPojoSerializer<?>) obj).actualSerializer);
        } else {
            return false;
        }
    }

    @Override
    public PojoSerializerSnapshot<T> snapshotConfiguration() {
        return actualSerializer.snapshotConfiguration();
    }
}
