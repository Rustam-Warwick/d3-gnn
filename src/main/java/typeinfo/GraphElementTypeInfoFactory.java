package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class GraphElementTypeInfoFactory extends TypeInfoFactory<GraphElement> {
    @Override
    public TypeInformation<GraphElement> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        Class clazz = TypeExtractionUtils.typeToClass(t);
        List<Field> fields = TypeExtractor.getAllDeclaredFields(clazz, false);
        PojoField[] pojoFields = new PojoField[fields.size()];
        for (int i = 0; i < fields.size(); i++) {

            pojoFields[i] = new PojoField(fields.get(i), TypeInformation.of(fields.get(i).getType()));
        }
        return new GraphElementTypeInfo<>(clazz, List.of(pojoFields));
    }
}
