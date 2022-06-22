package typeinfo;

import elements.GraphElement;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class GraphElementTypeInfo<T extends GraphElement> extends PojoTypeInfo<T> {
    public GraphElementTypeInfo(Class<T> typeClass, List<PojoField> fields) {
        super(typeClass, fields);
    }

}
