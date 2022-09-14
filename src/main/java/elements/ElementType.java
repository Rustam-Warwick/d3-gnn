package elements;


import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.ByteEnumTypeInfoFactory;

@TypeInfo(ByteEnumTypeInfoFactory.class)
public enum ElementType {
    PLUGIN,
    EDGE,
    FEATURE,
    VERTEX,
    HYPEREDGE,
    GRAPH,
    NONE

}
