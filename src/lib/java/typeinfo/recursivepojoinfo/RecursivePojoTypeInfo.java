package typeinfo.recursivepojoinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import typeinfo.listinfo.RecursiveListTypeInfo;

import java.util.List;

/**
 * TypeInformation for RecursivePOJO when there is a recursive List fields
 *
 * @param <T>
 */
public class RecursivePojoTypeInfo<T> extends PojoTypeInfo<T> {

    public RecursivePojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
        super(typeClass, fields);
        fields.forEach(item -> {
            if (item.getTypeInformation() instanceof RecursiveListTypeInfo) {
                ((RecursiveListTypeInfo) item.getTypeInformation()).elementTypeInfo = this;
            }
        });
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        TypeSerializer<T> mainSerializer = super.createSerializer(config);
        if (mainSerializer instanceof PojoSerializer) {
            return new RecursivePojoSerializer<>((PojoSerializer<T>) mainSerializer);
        }
        return mainSerializer;
    }

}
