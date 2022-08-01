package typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;

public class ByteEnumTypeInfo<T extends Enum<T>> extends EnumTypeInfo<T> {
    public ByteEnumTypeInfo(Class<T> typeClass) {
        super(typeClass);
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return new ByteEnumSerializer<>(getTypeClass());
    }
}
