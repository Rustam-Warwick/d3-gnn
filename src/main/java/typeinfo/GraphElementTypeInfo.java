package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class GraphElementTypeInfo<T extends GraphElement> extends PojoTypeInfo<T> {

    private final PojoField featuresRecField; // Recursive Field need to be careful
    private final PojoField[] fieldsArray;


    public GraphElementTypeInfo(Class<T> typeClass, List<PojoField> fields) throws Exception {
        super(typeClass, fields);
        Field featuresField = GraphElement.class.getField("features");
        featuresRecField = new PojoField(featuresField, new RecursiveListTypeInfo<>(this));
        fields.add(featuresRecField);
        fieldsArray = fields.toArray(new PojoField[fields.size()]);
        Arrays.sort(
                fieldsArray,
                new Comparator<PojoField>() {
                    @Override
                    public int compare(PojoField o1, PojoField o2) {
                        return o1.getField().getName().compareTo(o2.getField().getName());
                    }
                });
        int counterFields = 0;
        for (PojoField field : fields) {
            counterFields += field.getTypeInformation().getTotalFields();
        }

        Field fieldsField = PojoTypeInfo.class.getDeclaredField("fields");
        Field totalFieldsField = PojoTypeInfo.class.getDeclaredField("totalFields");
        fieldsField.setAccessible(true);
        totalFieldsField.setAccessible(true);

        fieldsField.set(this, fieldsArray);
        totalFieldsField.set(this, counterFields);


        fieldsField.setAccessible(false);
        totalFieldsField.setAccessible(false);
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[fieldsArray.length];
        Field[] reflectiveFields = new Field[fieldsArray.length];
        int recursiveIndex = -1;
        for (int i = 0; i < fieldsArray.length; i++) {
            if (fieldsArray[i] == featuresRecField) {
                recursiveIndex = i;
                reflectiveFields[recursiveIndex] = fieldsArray[recursiveIndex].getField();
                continue;
            }
            fieldSerializers[i] = fieldsArray[i].getTypeInformation().createSerializer(config);
            reflectiveFields[i] = fieldsArray[i].getField();
        }

        RecursiveListWrapperPojoSerializer<T> serializer = new RecursiveListWrapperPojoSerializer<T>(getTypeClass(), fieldSerializers, reflectiveFields, config, recursiveIndex);
        return serializer;
    }

}
