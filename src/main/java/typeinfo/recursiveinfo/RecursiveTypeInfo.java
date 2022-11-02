package typeinfo.recursiveinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import typeinfo.listinfo.RecursiveListTypeInfo;

import java.util.List;

/**
 * TypeInformation for RecursivePOJO when there is a recursive List fields
 * Delegates to POJO if no such field is found
 *
 * @param <T>
 * @todo Add recursive Set and Map fields as well
 */
public class RecursiveTypeInfo<T> extends PojoTypeInfo<T> {

    public RecursiveTypeInfo(Class<T> typeClass, List<PojoField> fields) {
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
            return new RecursiveSerializer<>((PojoSerializer<T>) mainSerializer);
        }
        return mainSerializer;
    }

}
