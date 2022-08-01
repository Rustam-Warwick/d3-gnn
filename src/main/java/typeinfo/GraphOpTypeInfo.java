package typeinfo;

import elements.GraphOp;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class GraphOpTypeInfo extends PojoTypeInfo<GraphOp> {
    private final List<PojoField> pojoFields;

    public GraphOpTypeInfo(Class<GraphOp> typeClass, List<PojoField> fields) {
        super(typeClass, fields);
        this.pojoFields = fields;
    }

    @Override
    @PublicEvolving
    @SuppressWarnings("unchecked")
    public TypeSerializer<GraphOp> createSerializer(ExecutionConfig config) {
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[pojoFields.size()];
        for (int i = 0; i < pojoFields.size(); i++) {
            fieldSerializers[i] = pojoFields.get(i).getTypeInformation().createSerializer(config);
        }
        return new GraphOpSerializer(fieldSerializers, config);
    }
}
