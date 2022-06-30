package elements.iterations;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.ByteEnumTypeInfoFactory;

/**
 * Helper class to define the part number of the sent data
 */

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum RemoteDestination {
    SELF,
    MASTER,
    REPLICAS,
    ALL
}
