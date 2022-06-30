package typeinfo;

import elements.OmitStorage;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.*;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

public class OmittingPojoTypeInfoFactory<T> extends TypeInfoFactory<T> {
    @Override
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        try {
            Class clazz = TypeExtractionUtils.typeToClass(t);
            List<Field> fields = TypeExtractor.getAllDeclaredFields(clazz, false);
            List<PojoField> pojoFields = new ArrayList<>();

            for (int i = 0; i < fields.size(); i++) {
                if(fields.get(i).isAnnotationPresent(OmitStorage.class))continue;
                Type fieldType = fields.get(i).getGenericType();
                try {
                    pojoFields.add(new PojoField(fields.get(i), TypeExtractor.createTypeInfo(fieldType)));
                } catch (Exception e) {
                    Class<?> genericClass = Object.class;
                    if (isClassType(fieldType)) {
                        genericClass = typeToClass(fieldType);
                    }
                    pojoFields.add(new PojoField(fields.get(i), new GenericTypeInfo<>(genericClass)));
                }
            }
            return new PojoTypeInfo<>(clazz, pojoFields);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
