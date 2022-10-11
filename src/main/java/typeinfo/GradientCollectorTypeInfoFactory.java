package typeinfo;

import ai.djl.ndarray.NDArray;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

public class GradientCollectorTypeInfoFactory<T> extends TypeInfoFactory<Map<T, NDArray>> {
    @Override
    public TypeInformation<Map<T, NDArray>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new GradientCollectorTypeInfo<T>((TypeInformation<T>)genericParameters.get("T"));
    }
}
