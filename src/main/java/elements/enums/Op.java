package elements.enums;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum Op {
    COMMIT,
    REMOVE,

    SYNC,

    SYNC_REQUEST,

    RMI,
    OPERATOR_EVENT,
}
