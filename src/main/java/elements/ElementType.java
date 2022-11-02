package elements;


import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum ElementType {
    PLUGIN,
    EDGE,
    STANDALONE_FEATURE,
    ATTACHED_FEATURE,
    VERTEX,
    HYPEREDGE,
    GRAPH,
    NONE
}
