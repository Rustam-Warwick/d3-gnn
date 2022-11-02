package typeinfo.setinfo;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

public class SetTypeInfoFactory<T> extends TypeInfoFactory<Set<T>> {
    @Override
    public TypeInformation<Set<T>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new SetTypeInfo<T>((TypeInformation<T>) genericParameters.get("E"));
    }
}
