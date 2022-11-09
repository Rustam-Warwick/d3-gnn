package elements.enums;


import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum MessageDirection {
    FORWARD,
    ITERATE,
    BACKWARD,
}
