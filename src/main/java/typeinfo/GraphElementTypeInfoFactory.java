package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

public class GraphElementTypeInfoFactory extends TypeInfoFactory<GraphElement> {
    @Override
    public TypeInformation<GraphElement> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        try{
        Class clazz = TypeExtractionUtils.typeToClass(t);
        List<Field> fields = TypeExtractor.getAllDeclaredFields(clazz, false);
        PojoField[] pojoFields = new PojoField[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            if(fields.get(i).getName().equals("features"))continue;
            Type fieldType = fields.get(i).getGenericType();
            try{
                pojoFields[i] = new PojoField(fields.get(i),TypeExtractor.createTypeInfo(fieldType));
            }catch (Exception e){
                Class<?> genericClass = Object.class;
                if (isClassType(fieldType)) {
                    genericClass = typeToClass(fieldType);
                }
                pojoFields[i] = new PojoField(fields.get(i), new GenericTypeInfo<>(genericClass));
            }
        }
            return new GraphElementTypeInfo<>(clazz, List.of(pojoFields));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
