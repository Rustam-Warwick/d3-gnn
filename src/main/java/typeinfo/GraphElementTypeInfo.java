package typeinfo;

import elements.Feature;
import elements.GraphElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import javax.swing.*;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class GraphElementTypeInfo<T extends GraphElement> extends PojoTypeInfo<T> {
    public GraphElementTypeInfo(Class<T> typeClass, List<PojoField> fields) throws Exception {
        super(typeClass, fields);
        Field featuresField = GraphElement.class.getField("features");
        fields.add(new PojoField(featuresField, new ListTypeInfo<>(this)));
        PojoField[] fieldsArray = fields.toArray(new PojoField[fields.size()]);
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
        fieldsField.set(this, fieldsArray);
        totalFieldsField.set(this, counterFields);
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return super.createSerializer(config);
    }
}
