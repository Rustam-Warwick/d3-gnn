package elements;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum Op {
    NONE,
    COMMIT,
    REMOVE,
    SYNC,
    RMI,
    OPERATOR_EVENT,
}
