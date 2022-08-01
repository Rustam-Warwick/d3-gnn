package typeinfo;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class GraphOpTypeInfoFactory extends TypeInfoFactory<GraphOp> {

    @Override
    public TypeInformation<GraphOp> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        List<Field> fields = TypeExtractor.getAllDeclaredFields(TypeExtractionUtils.typeToClass(t), false);
        PojoField[] pojoFields = new PojoField[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fields.get(i).getGenericType();
            pojoFields[i] = new PojoField(fields.get(i), TypeExtractor.createTypeInfo(fieldType));
        }
        return new GraphOpTypeInfo(GraphOp.class, List.of(pojoFields));
    }
}
