package typeinfo;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class GraphOpTypeInfoFactory extends TypeInfoFactory<GraphOp> {


    @Override
    public TypeInformation<GraphOp> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        Field[] fields = GraphOp.class.getFields();
        PojoField[] pojoFields = new PojoField[fields.length];
        for (int i = 0; i < fields.length; i++) {
            pojoFields[i] = new PojoField(fields[i], TypeInformation.of(fields[i].getType()));
        }
        return new GraphOpTypeInfo(GraphOp.class, List.of(pojoFields));
    }
}
