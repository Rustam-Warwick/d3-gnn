package elements.iterations;


import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum MessageDirection {
    FORWARD,
    ITERATE,
    BACKWARD,
}
