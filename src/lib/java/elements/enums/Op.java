package elements.enums;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

/**
 * COMMIT -> Create if DNE or update
 * ADD -> Assuming DNE simply create
 * REMOVE -> Remove
 * SYNC -> Sync to replica
 * SYNC_REQUEST -> Replica sends Sync to Master
 * RMI -> Remote Method Invocation request
 * OPERATOR_EVENT -> Sending OperatorEvents
 */
@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum Op {
    COMMIT,

    ADD,

    REMOVE,

    SYNC,

    SYNC_REQUEST,

    RMI,

    OPERATOR_EVENT,
}
