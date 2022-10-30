package typeinfo;

import elements.OmitStorage;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.*;

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
public class RecursiveTypeInfoFactory<T> extends TypeInfoFactory<T> {
    @Override
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return this.createTypeInfo(t, genericParameters, false);
    }


    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters, boolean omitStorage) {
        try {
            Class clazz = TypeExtractionUtils.typeToClass(t);
            List<Field> fields = TypeExtractor.getAllDeclaredFields(clazz, false);
            List<PojoField> pojoFields = new ArrayList<>();
            List<Field> recursiveListFields = new ArrayList<>();

            for (Field field : fields) {
                if (omitStorage && field.isAnnotationPresent(OmitStorage.class)) continue;
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
                // Fallback to Pojo otherwise it will not serialize properly
                Constructor<TypeExtractor> construc = TypeExtractor.class.getDeclaredConstructor();
                construc.setAccessible(true);
                TypeExtractor ex = construc.newInstance();
                Method analyzePojo = TypeExtractor.class.getDeclaredMethod("analyzePojo", Type.class, List.class, TypeInformation.class, TypeInformation.class);
                analyzePojo.setAccessible(true);
                PojoTypeInfo<T> pojoType =
                        (PojoTypeInfo<T>) analyzePojo.invoke(ex, t, new ArrayList<>(List.of(t)), null, null);

                if (omitStorage) {
                    // PojoTypeInfo does not know about the
                    Field fieldsPojoField = PojoTypeInfo.class.getDeclaredField("fields");
                    fieldsPojoField.setAccessible(true);
                    PojoField[] fieldsPojo = (PojoField[]) fieldsPojoField.get(pojoType);
                    List<PojoField> pojoFieldsOmitted = new ArrayList<>();
                    for (PojoField pojoField : fieldsPojo) {
                        if (omitStorage && pojoField.getField().isAnnotationPresent(OmitStorage.class)) continue;
                        pojoFieldsOmitted.add(pojoField);
                    }
                    pojoType = new PojoTypeInfo(clazz, pojoFieldsOmitted);
                    fieldsPojoField.setAccessible(false);
                }

                construc.setAccessible(false);
                analyzePojo.setAccessible(false);
                if (pojoType == null) {
                    throw new IllegalStateException("bla-bla");
                }
                return pojoType;
            }

            return new RecursiveTypeInfo<>(clazz, pojoFields, recursiveListFields);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
