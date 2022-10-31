package typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class RecursiveTypeInfo<T> extends PojoTypeInfo<T> {

    private final PojoField[] fieldsArray;


    public RecursiveTypeInfo(Class<T> typeClass, List<PojoField> fields, List<Field> recursiveFields) throws Exception {
        super(typeClass, fields);
        // Populate the recursive fields if exists
        recursiveFields.forEach(item -> {
            fields.add(new PojoField(item, new RecursiveListTypeInfo<>(this)));
        });
        fieldsArray = fields.toArray(new PojoField[fields.size()]);
        Arrays.sort(
                fieldsArray,
                new Comparator<PojoField>() {
                    @Override
                    public int compare(PojoField o1, PojoField o2) {
                        return o1.getField().getName().compareTo(o2.getField().getName());
                    }
                });
        // Modify the counters
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
        for (int i = 0; i < fieldsArray.length; i++) {
            if (!(fieldsArray[i].getTypeInformation() instanceof RecursiveListTypeInfo)) {
                // Do not populate recursive field serializer, will be done in actual serializer constructor
                fieldSerializers[i] = fieldsArray[i].getTypeInformation().createSerializer(config);
            }
            reflectiveFields[i] = fieldsArray[i].getField();
        }

        RecursiveSerializer<T> serializer = new RecursiveSerializer<T>(getTypeClass(), fieldSerializers, reflectiveFields, config);
        return serializer;
    }

}
