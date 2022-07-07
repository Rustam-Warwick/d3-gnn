package typeinfo;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * Special Factory that recursively serializer List Fields, if no recursion list is found will fall back to PojoTypeInfo
 */
public class RecursiveListFieldsTypeInfoFactory<T> extends TypeInfoFactory<T> {

    @Override
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        try {
            Class clazz = TypeExtractionUtils.typeToClass(t);
            List<Field> fields = TypeExtractor.getAllDeclaredFields(clazz, false);
            List<PojoField> pojoFields = new ArrayList<>();
            List<Field> recursiveListFields = new ArrayList<>();

            for (Field field : fields) {
                Type fieldType = field.getGenericType();
                try {
                    if (field.getType().equals(List.class) &&
                            TypeExtractionUtils.typeToClass(TypeExtractionUtils.extractTypeArgument(field.getGenericType(), 0)).isAssignableFrom(clazz)) {
                        // This is a recursive list field
                        recursiveListFields.add(field);
                    } else {
                        final TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(fieldType);
                        pojoFields.add(new PojoField(field, typeInfo));
                    }
                } catch (InvalidTypesException e) {
                    Class<?> genericClass = Object.class;
                    if (isClassType(fieldType)) {
                        genericClass = typeToClass(fieldType);
                    }
                    pojoFields.add(
                            new PojoField(field, new GenericTypeInfo<>(genericClass)));
                }
            }
            if (recursiveListFields.isEmpty()) {
                // Fallback to Pojo
                Constructor<TypeExtractor> construc = TypeExtractor.class.getDeclaredConstructor();
                construc.setAccessible(true);
                TypeExtractor ex = construc.newInstance();
                Method analyzePojo = TypeExtractor.class.getDeclaredMethod("analyzePojo", Type.class, List.class, TypeInformation.class, TypeInformation.class);
                analyzePojo.setAccessible(true);
                TypeInformation<T> pojoType =
                        (TypeInformation<T>) analyzePojo.invoke(ex, t, new ArrayList<>(List.of(t)), null, null);
                construc.setAccessible(false);
                analyzePojo.setAccessible(false);
                if (pojoType == null) {
                    throw new IllegalStateException("bla-bla");
                }
                return pojoType;
            }

            return new RecursiveListFieldTypeInfo<>(clazz, pojoFields, recursiveListFields);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
