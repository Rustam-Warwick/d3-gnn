package typeinfo;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Type information factory for List<E> interfaces
 *
 * @param <T>
 */
public class ListTypeInformationFactory<T> extends TypeInfoFactory<List<T>> {

    @Override
    public TypeInformation<List<T>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new ListTypeInfo(genericParameters.get("E"));
    }
}
