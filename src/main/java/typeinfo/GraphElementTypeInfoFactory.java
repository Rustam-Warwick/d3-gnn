package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

public class GraphElementTypeInfoFactory extends TypeInfoFactory<GraphElement> {
    @Override
    public TypeInformation<GraphElement> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return null;
    }
}
