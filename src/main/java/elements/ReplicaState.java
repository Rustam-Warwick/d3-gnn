package elements;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum ReplicaState {
    UNDEFINED,
    REPLICA,
    MASTER
}
