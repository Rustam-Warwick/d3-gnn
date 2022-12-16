package typeinfo.setinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TypeInformation for Set, and HashSets
 *
 * @param <T> Type of the underlying element
 */
public class SetTypeInfo<T> extends TypeInformation<Set<T>> {
    private final TypeInformation<T> elementTypeInfo;

    public SetTypeInfo(Class<T> elementTypeClass) {
        this.elementTypeInfo = of(checkNotNull(elementTypeClass, "elementTypeClass"));
    }

    public SetTypeInfo(TypeInformation<T> elementTypeInfo) {
        this.elementTypeInfo = checkNotNull(elementTypeInfo, "elementTypeInfo");
    }

    // ------------------------------------------------------------------------
    //  SetTypeInfo specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the type information for the elements contained in the list
     */
    public TypeInformation<T> getElementTypeInfo() {
        return elementTypeInfo;
    }

    // ------------------------------------------------------------------------
    //  TypeInformation implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        // similar as arrays, the lists are "opaque" to the direct field addressing logic
        // since the list's elements are not addressable, we do not expose them
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Set<T>> getTypeClass() {
        return (Class<Set<T>>) (Class<?>) List.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<T>> createSerializer(ExecutionConfig config) {
        TypeSerializer<T> elementTypeSerializer = elementTypeInfo.createSerializer(config);
        return new SetSerializer<>(elementTypeSerializer);
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "SET<" + elementTypeInfo + '>';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof SetTypeInfo) {
            final SetTypeInfo<?> other = (SetTypeInfo<?>) obj;
            return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * elementTypeInfo.hashCode() + 1;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }
}
