package typeinfo.recursiveinfo;

import elements.OmitStorage;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.*;
import typeinfo.listinfo.RecursiveListTypeInfo;
import typeinfo.setinfo.SetTypeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * Special Factory that recursively serializer List Fields, if no recursion list is found will fall back to PojoTypeInfo
 *
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
            for (Field field : fields) {
                if (omitStorage && field.isAnnotationPresent(OmitStorage.class)) continue;
                Type fieldType = field.getGenericType();
                try {
                    if (List.class.isAssignableFrom(field.getType())){
                        // This is List field
                        if(TypeExtractionUtils.typeToClass(TypeExtractionUtils.extractTypeArgument(fieldType, 0)).isAssignableFrom(clazz)) {
                            // Recursive List Field
                            pojoFields.add(new PojoField(field, new RecursiveListTypeInfo<>(null))); // Will be populated in RecursiveTypeInfoFactory
                        }else{
                            // Non-Recursive List Field
                            final TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(TypeExtractionUtils.extractTypeArgument(fieldType,0));
                            pojoFields.add(new PojoField(field, new ListTypeInfo<>(typeInfo)));
                        }
                    }else if(Set.class.isAssignableFrom(field.getType())){
                        // This is Set Field
                        final TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(TypeExtractionUtils.extractTypeArgument(fieldType,0));
                        pojoFields.add(new PojoField(field, new SetTypeInfo<>(typeInfo)));
                    }else {
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
            return new RecursiveTypeInfo<T>(clazz, pojoFields);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
