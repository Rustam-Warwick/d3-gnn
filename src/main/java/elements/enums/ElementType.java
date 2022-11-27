package elements.enums;


import org.apache.flink.api.common.typeinfo.TypeInfo;
import typeinfo.byteinfo.ByteEnumTypeInfoFactory;

/**
 * Types of {@link elements.GraphElement}
 */
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
